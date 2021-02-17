#ifndef SRC_MULTITHREADEDPAGERANKCOMPUTER_HPP_
#define SRC_MULTITHREADEDPAGERANKCOMPUTER_HPP_

#include <atomic>
#include <mutex>
#include <thread>

#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "immutable/network.hpp"
#include "immutable/pageIdAndRank.hpp"
#include "immutable/pageRankComputer.hpp"

class MultiThreadedPageRankComputer : public PageRankComputer {
public:
    MultiThreadedPageRankComputer(uint32_t numThreadsArg) : numThreads(numThreadsArg),
                                                            barrier(numThreadsArg + 1) {};

    std::vector<PageIdAndRank>
    computeForNetwork(Network const& network, double alpha, uint32_t iterations, double tolerance) const {

        generateIds(network);

        pageHashMap = pageRanksMap();
        currentPageHashMap = pageRanksMap();
        numLinks = std::unordered_map<PageId, uint32_t, PageIdHash>();
        danglingNodes = std::unordered_set<PageId, PageIdHash>();

        PageRank initialValue = 1.0 / network.getSize();


        for (Page const& page : network.getPages()) {
            pageHashMap[page.getId()] = initialValue;
            numLinks[page.getId()] = page.getLinks().size();
            if (page.getLinks().size() == 0) {
                danglingNodes.insert(page.getId());
            }
        }


        // Bucket sort on edges, sorting by target of the link
        std::unordered_map<PageId, std::vector<std::pair<PageId, PageId>>, PageIdHash> edgesVectors;
        for (auto page : network.getPages()) {
            for (auto link : page.getLinks()) {
                edgesVectors[link].emplace_back(page.getId(), link);
            }
        } // todo not optimal?

        pageIdPairs edges;
        for (auto links : edgesVectors) {
            edges.insert(edges.end(), links.second.begin(), links.second.end());
        }

        nextIteration = true;

        std::vector<std::thread> threads;
        std::vector<PageIdAndRank> result;

        for (uint32_t i = 0; i < numThreads; ++i) {
            threads.emplace_back(
                    std::thread(&MultiThreadedPageRankComputer::computePageRank, this, i, std::ref(edges),
                                alpha));
        }

        double danglingWeight = initialValue;
        bool tooManyIterations = true;

        for (uint32_t i = 0; i < iterations; ++i) {
            double dangleSum = 0;
            for (auto& danglingNode : danglingNodes) { // todo auto?
                dangleSum += pageHashMap[danglingNode];
            }
            dangleSum = dangleSum * alpha;


            { // Add dangle sums and constant values to global map
                std::unique_lock<std::mutex> mutex_lock(mutex);
                for (auto& pageMapElem : pageHashMap) {
                    PageId pageId = pageMapElem.first;

                    if (currentPageHashMap.find(pageId) == currentPageHashMap.end()) {
                        currentPageHashMap[pageId] =
                                dangleSum * danglingWeight + (1.0 - alpha) / network.getSize();
                    } else {
                        currentPageHashMap[pageId] +=
                                dangleSum * danglingWeight + (1.0 - alpha) / network.getSize();
                    }
                }
            }
            double difference = 0;

            barrier.reach();

            for (auto& pageMapElem : pageHashMap) {
                PageId pageId = pageMapElem.first; // Other threads at barrier, safe to read
                difference += std::abs(pageHashMap[pageId] - currentPageHashMap[pageId]);
            }

            nextIteration = difference >= tolerance;

            if (!nextIteration) {
                for (auto iter : currentPageHashMap) {
                    result.emplace_back(PageIdAndRank(iter.first, iter.second));
                }
                tooManyIterations = false;
                i = iterations;
            } else {
                pageHashMap = currentPageHashMap;
                currentPageHashMap = pageRanksMap();
            }

            barrier.reach();

        }


        for (uint32_t i = 0; i < numThreads; ++i) {
            threads[i].join();
        }

        ASSERT(result.size() == network.getSize(),
               "Invalid result size=" << result.size() << ", for network" << network);
        ASSERT(!tooManyIterations, "Not able to find result in iterations=" << iterations);

        return result;
    }

    std::string getName() const {
        return "MultiThreadedPageRankComputer[" + std::to_string(this->numThreads) + "]";
    }

private:
    using pageIdPairs = std::vector<std::pair<PageId, PageId>>;

    using pageRanksMap = std::unordered_map<PageId, PageRank, PageIdHash>;

    class Barrier {
    public:
        explicit Barrier(uint32_t resistance) : resistance(resistance), waiting(0), generation(0) {}

        void reach() {
            std::unique_lock<std::mutex> mutex_lock{cv_mutex};
            std::size_t current_generation = generation;

            if (++waiting < resistance) {
                waiting_threads.wait(mutex_lock,
                                     [this, current_generation] { return current_generation != generation; });
            } else {
                generation++;
                waiting = 0;
                waiting_threads.notify_all();
            }
        }

    private:
        std::mutex cv_mutex;
        std::condition_variable waiting_threads;
        uint32_t resistance;
        uint32_t waiting;
        std::size_t generation;
    };

    uint32_t numThreads;
    // todo name types

    mutable pageRanksMap pageHashMap;
    mutable pageRanksMap currentPageHashMap;
    mutable std::unordered_map<PageId, uint32_t, PageIdHash> numLinks;
    mutable std::unordered_set<PageId, PageIdHash> danglingNodes;

    mutable Barrier barrier;
    mutable std::mutex mutex;

    mutable bool nextIteration;

    void generateIds(Network const& network) const {
        std::vector<std::thread> threads;

        for (uint32_t i = 0; i < numThreads; ++i) {
            threads.emplace_back(
                    std::thread(&MultiThreadedPageRankComputer::generateIdsThread, this, i, std::ref(network)));
        }

        for (uint32_t i = 0; i < numThreads; ++i) {
            threads[i].join();
        }
    }

    void generateIdsThread(uint32_t threadNum, Network const& network) const {
        size_t pagesPerThread = network.getSize() / numThreads;
        std::vector<Page> const& pages = network.getPages();

        auto beginning = pages.begin() + threadNum * pagesPerThread; // todo auto?
        auto end = beginning + pagesPerThread;
        if (threadNum == numThreads - 1) {
            end = pages.end();
        }

        while (beginning != end) {
            beginning->generateId(network.getGenerator());
            beginning++;
        }
    }

    void computePageRank(uint32_t threadNum, pageIdPairs const& edges, double alpha) const {
        size_t edgesPerThread = edges.size() / numThreads;

        auto beginning = edges.begin() + threadNum * edgesPerThread; // todo auto?
        auto end = beginning + edgesPerThread;
        if (threadNum == numThreads - 1) {
            end = edges.end();
        }



        while (nextIteration) {
            pageRanksMap rankMap;

            auto current = beginning;
            while (current != end) {
                PageId source = current->first;
                PageId target = current->second;

                if (rankMap.find(target) == rankMap.end()) {
                    rankMap[target] = 0.0;
                }

                // Read only in this section, thread safe
                rankMap[target] += alpha * pageHashMap[source] / numLinks[source];

                current++;
            }

            { // Add rank sums to global map
                std::unique_lock<std::mutex> mutex_lock(mutex);
                for (auto idRank : rankMap) {
                    PageId pageId = idRank.first;
                    PageRank rank = idRank.second;

                    if (currentPageHashMap.find(pageId) == currentPageHashMap.end()) {
                        currentPageHashMap[pageId] = rank;
                    } else {
                        currentPageHashMap[pageId] += rank;
                    }
                }
            }

            barrier.reach();
            barrier.reach();
        }

    }


};

#endif /* SRC_MULTITHREADEDPAGERANKCOMPUTER_HPP_ */
