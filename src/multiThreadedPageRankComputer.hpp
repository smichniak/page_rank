#ifndef SRC_MULTITHREADEDPAGERANKCOMPUTER_HPP_
#define SRC_MULTITHREADEDPAGERANKCOMPUTER_HPP_

#include <atomic>
#include <mutex>
#include <condition_variable>
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
                                                            barrier(numThreadsArg + 1), dangleSum(0) {};

    std::vector<PageIdAndRank>
    computeForNetwork(Network const& network, double alpha, uint32_t iterations, double tolerance) const {
        pageHashMap = pageRanksMap();
        currentPageHashMap = pageRanksMap();
        numLinks = std::unordered_map<PageId, uint32_t, PageIdHash>();
        difference = 0;

        generateIds(network);

        // Bucket sort on edges, sorting by target of the link
        std::unordered_map<PageId, std::vector<std::pair<PageId, PageId>>, PageIdHash> edgesVectors;
        for (auto page : network.getPages()) {
            pageHashMap[page.getId()] = 1.0 / network.getSize();
            numLinks[page.getId()] = page.getLinks().size();

            for (auto link : page.getLinks()) {
                edgesVectors[link].emplace_back(page.getId(), link);
            }
        } // todo not optimal?

        pageIdPairs edges;
        for (auto links : edgesVectors) {
            edges.insert(edges.end(), links.second.begin(), links.second.end());
        }

        nextIteration = true;
        bool tooManyIterations = true;

        std::vector<std::thread> threads;
        std::vector<PageIdAndRank> result;

        for (uint32_t i = 0; i < numThreads; ++i) {
            threads.emplace_back(
                    std::thread(&MultiThreadedPageRankComputer::computePageRank, this, i, std::ref(edges),
                                alpha, std::ref(network)));
        }


        for (uint32_t i = 0; i < iterations; ++i) {
            barrier.reach();
            barrier.reach();
            barrier.reach();

            nextIteration = difference >= tolerance;

//            std::cout << currentRankSum << '\n';
//            for (auto iter : pageHashMap) {
//                std::cout << iter.second << ' ';
//            }
//            std::cout << '\n';

            if (!nextIteration) {
                for (auto iter : currentPageHashMap) {
                    result.emplace_back(PageIdAndRank(iter.first, iter.second));
                }
                tooManyIterations = false;
                i = iterations;
            } else {
                pageHashMap = currentPageHashMap;
                currentPageHashMap = pageRanksMap();
                difference = 0;
                dangleSum = 0;
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

    mutable Barrier barrier;
    mutable std::mutex mutex;

    mutable bool nextIteration;
    mutable double dangleSum;
    mutable double difference;

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

    void computePageRank(uint32_t threadNum, pageIdPairs const& edges, double alpha, Network const& network) const {
        size_t pagesPerThread = network.getSize() / numThreads;
        std::vector<Page> const& pages = network.getPages();

        auto beginningPages = pages.begin() + threadNum * pagesPerThread; // todo auto?
        auto endPages = beginningPages + pagesPerThread;


        size_t edgesPerThread = edges.size() / numThreads;
        auto beginningEdges = edges.begin() + threadNum * edgesPerThread; // todo auto?
        auto endEdges = beginningEdges + edgesPerThread;

        if (threadNum == numThreads - 1) {
            endPages = pages.end();
            endEdges = edges.end();
        }

        double danglingWeight = 1.0 / network.getSize();

        while (nextIteration) {
            pageRanksMap rankMap;
            double localDangleSum = 0;
            double localDifference = 0;

            auto currentPages = beginningPages;
            while (currentPages != endPages) {
                rankMap[currentPages->getId()] = (1.0 - alpha) / network.getSize();
                if (numLinks[currentPages->getId()] == 0) {
                    localDangleSum += pageHashMap[currentPages->getId()]; // Read only, thread safe
                }

                currentPages++;
            }

            { // Add dangle sum to global counter
                std::unique_lock<std::mutex> mutex_lock(mutex);
                dangleSum += localDangleSum;
            }

            barrier.reach();

            currentPages = beginningPages;
            while (currentPages != endPages) {
                rankMap[currentPages->getId()] += dangleSum * alpha * danglingWeight; // Read only, thread safe
                currentPages++;
            }


            auto currentEdges = beginningEdges;
            while (currentEdges != endEdges) {
                PageId source = currentEdges->first;
                PageId target = currentEdges->second;

                if (rankMap.find(target) == rankMap.end()) {
                    rankMap[target] = 0.0;
                }
                // Read only in this section, thread safe
                rankMap[target] += alpha * pageHashMap[source] / numLinks[source];

                currentEdges++;
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

            currentPages = beginningPages;
            while (currentPages != endPages) {
                localDifference += std::abs(pageHashMap[currentPages->getId()] - currentPageHashMap[currentPages->getId()]);
                currentPages++;
            }

            { // Add dangle sum to global counter
                std::unique_lock<std::mutex> mutex_lock(mutex);
                difference += localDifference;
            }

            barrier.reach();
            barrier.reach();
        }

    }


};

#endif /* SRC_MULTITHREADEDPAGERANKCOMPUTER_HPP_ */
