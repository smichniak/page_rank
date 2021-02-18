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
                                                            barrier(numThreadsArg + 1) {};

    std::vector<PageIdAndRank>
    computeForNetwork(Network const& network, double alpha, uint32_t iterations, double tolerance) const {
        pageHashMap = pageRanksMap();
        currentPageHashMap = pageRanksMap();
        numLinks = std::unordered_map<PageId, uint32_t, PageIdHash>();
        dangleSum = 0;
        difference = 0;
        nextIteration = true;

        generateIds(network);

        PageRank initialRank = 1.0 / network.getSize();

        // Bucket sort on edges, sorting by target of the link
        std::unordered_map<PageId, std::vector<std::pair<PageId, PageId>>, PageIdHash> edgesVectors;
        for (auto page : network.getPages()) {
            pageHashMap[page.getId()] = initialRank;
            currentPageHashMap[page.getId()] = 0;
            numLinks[page.getId()] = page.getLinks().size();

            for (auto link : page.getLinks()) {
                edgesVectors[link].emplace_back(page.getId(), link);
            }
        }

        pageIdPairs edges;
        for (auto links : edgesVectors) {
            edges.insert(edges.end(), links.second.begin(), links.second.end());
        }

        std::vector<std::thread> threads;

        for (uint32_t i = 0; i < numThreads; ++i) {
            threads.emplace_back(
                    std::thread(&MultiThreadedPageRankComputer::computePageRank, this, i, std::ref(edges),
                                alpha, std::ref(network)));
        }

        std::vector<PageIdAndRank> result;
        bool tooManyIterations = true;

        for (uint32_t i = 0; i < iterations; ++i) {
            barrier.reach(); // Barrier 1
            barrier.reach(); // Barrier 2

            nextIteration = difference >= tolerance;

            if (!nextIteration) {
                for (auto iter : pageHashMap) {
                    result.emplace_back(PageIdAndRank(iter.first, iter.second));
                }
                tooManyIterations = false;
                i = iterations; // Force loop exit
            } else if (i == iterations - 1) {
                nextIteration = false;
            } else {
                difference = 0;
                dangleSum = 0;
            }

            barrier.reach(); // Barrier 3
        }

        for (uint32_t i = 0; i < numThreads; ++i) {
            threads[i].join();
        }

        ASSERT(!tooManyIterations, "Not able to find result in iterations=" << iterations);

        ASSERT(result.size() == network.getSize(),
               "Invalid result size=" << result.size() << ", for network" << network);

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

    void
    computePageRank(uint32_t threadNum, pageIdPairs const& edges, double alpha, Network const& network) const {
        std::vector<Page> const& pages = network.getPages();

        size_t pagesPerThread = network.getSize() / numThreads;
        auto beginningPages = pages.begin() + threadNum * pagesPerThread;
        auto endPages = beginningPages + pagesPerThread;

        size_t edgesPerThread = edges.size() / numThreads;
        auto beginningEdges = edges.begin() + threadNum * edgesPerThread;
        auto endEdges = beginningEdges + edgesPerThread;

        if (threadNum == numThreads - 1) {
            endPages = pages.end();
            endEdges = edges.end();
        }

        double danglingWeight = 1.0 / network.getSize();

        std::unordered_set<PageId, PageIdHash> danglingNodes;

        auto currentPage = beginningPages;
        while (currentPage != endPages) {
            if (numLinks[currentPage->getId()] == 0) { // Read only, thread safe
                danglingNodes.insert(currentPage->getId());
            }
            currentPage++;
        }

        while (nextIteration) {
            pageRanksMap rankMap;
            double localDangleSum = 0;
            double localDifference = 0;

            for (auto danglingNode : danglingNodes) {
                localDangleSum += pageHashMap[danglingNode];
            }

            if (localDangleSum != 0) {
                // Add dangle sum to global counter
                std::unique_lock<std::mutex> mutex_lock(mutex);
                dangleSum += localDangleSum;
            }

            auto currentEdge = beginningEdges;
            while (currentEdge != endEdges) {
                PageId source = currentEdge->first;
                PageId target = currentEdge->second;

                // Read only in this section, thread safe
                rankMap[target] += alpha * pageHashMap[source] / numLinks[source];

                currentEdge++;
            }

            { // Add rank sums to global map
                std::unique_lock<std::mutex> mutex_lock(mutex);
                for (auto idRank : rankMap) {
                    PageId pageId = idRank.first;
                    PageRank rank = idRank.second;

                    currentPageHashMap[pageId] += rank;
                }
            }

            barrier.reach(); // Barrier 1

            currentPage = beginningPages;
            while (currentPage != endPages) { // Different threads accessing different pages, thread safe
                auto currentRankIter = currentPageHashMap.find(currentPage->getId());
                auto oldRankIter = pageHashMap.find(currentPage->getId());

                PageRank currentRank = currentRankIter->second + (1.0 - alpha) / network.getSize() +
                                       dangleSum * alpha * danglingWeight;

                localDifference += std::abs(oldRankIter->second - currentRank);

                oldRankIter->second = currentRank;
                currentRankIter->second = 0;

                currentPage++;
            }

            { // Add local difference to global counter
                std::unique_lock<std::mutex> mutex_lock(mutex);
                difference += localDifference;
            }

            barrier.reach(); // Barrier 2
            barrier.reach(); // Barrier 3
        }

    }


};

#endif /* SRC_MULTITHREADEDPAGERANKCOMPUTER_HPP_ */
