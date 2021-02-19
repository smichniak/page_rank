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
        resetComputer();

        generateIds(network);

        pageIdPairs edges;
        initializeEdges(network, edges);

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

    using pageRankMap = std::unordered_map<PageId, PageRank, PageIdHash>;

    using pageIteratorPair = std::pair<std::vector<Page const>::iterator, std::vector<Page const>::iterator>;

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

    mutable pageRankMap pageHashMap;
    mutable pageRankMap currentPageHashMap;
    mutable std::unordered_map<PageId, uint32_t, PageIdHash> numLinks;

    mutable Barrier barrier;
    mutable std::mutex mutex;

    mutable bool nextIteration;
    mutable double dangleSum;
    mutable double difference;

    void resetComputer() const {
        pageHashMap = pageRankMap();
        currentPageHashMap = pageRankMap();
        numLinks = std::unordered_map<PageId, uint32_t, PageIdHash>();
        dangleSum = 0;
        difference = 0;
        nextIteration = true;
    }

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
        std::vector<Page> const& pages = network.getPages();

        auto iteratorPair = divideVector(pages, threadNum);

        for (auto current = iteratorPair.first; current != iteratorPair.second; ++current)
            current->generateId(network.getGenerator());
    }

    void initializeEdges(Network const& network, pageIdPairs& edgesVectors) const {
        PageRank initialRank = 1.0 / network.getSize();

        for (auto const& page : network.getPages()) {
            pageHashMap[page.getId()] = initialRank;
            currentPageHashMap[page.getId()] = 0;
            numLinks[page.getId()] = page.getLinks().size();

            for (auto const& link : page.getLinks()) {
                edgesVectors.emplace_back(page.getId(), link);
            }
        }
    }

    template<typename T>
    std::pair<typename std::vector<T const>::iterator, typename std::vector<T const>::iterator>
    divideVector(std::vector<T> const& vector, uint32_t threadNum) const {
        size_t sizePerThread = vector.size() / numThreads;
        auto beginning = vector.begin() + threadNum * sizePerThread;
        auto end = beginning + sizePerThread;
        if (threadNum == numThreads - 1) {
            end = vector.end();
        }
        return std::make_pair(beginning, end);
    }

    void getDanglingNodes(pageIteratorPair iteratorPairPages,
                          std::unordered_set<PageId, PageIdHash>& danglingNodes) const {
        for (auto currentPage = iteratorPairPages.first;
             currentPage != iteratorPairPages.second; ++currentPage) {
            if (numLinks[currentPage->getId()] == 0) { // Read only, thread safe
                danglingNodes.insert(currentPage->getId());
            }
        }
    }

    // Add rank sums to global map
    void addRankSums(pageRankMap const& rankMap) const {
        std::unique_lock<std::mutex> mutex_lock(mutex);
        for (auto idRank : rankMap) {
            PageId pageId = idRank.first;
            PageRank rank = idRank.second;

            currentPageHashMap[pageId] += rank;
        }
    }

    PageRank getLocalDifference(pageIteratorPair iteratorPairPages, double alpha, size_t networkSize) const {
        PageRank localDifference = 0;
        double danglingWeight = 1.0 / networkSize;

        for (auto currentPage = iteratorPairPages.first;
             currentPage != iteratorPairPages.second; ++currentPage) {
            // Different threads accessing different pages, thread safe
            auto currentRankIter = currentPageHashMap.find(currentPage->getId());
            auto oldRankIter = pageHashMap.find(currentPage->getId());

            PageRank currentRank = currentRankIter->second + (1.0 - alpha) / networkSize +
                                   dangleSum * alpha * danglingWeight;

            localDifference += std::abs(oldRankIter->second - currentRank);

            oldRankIter->second = currentRank;
            currentRankIter->second = 0;
        }

        return localDifference;
    }

    void
    computePageRank(uint32_t threadNum, pageIdPairs const& edges, double alpha, Network const& network) const {
        std::vector<Page> const& pages = network.getPages();

        pageIteratorPair iteratorPairPages = divideVector(pages, threadNum);
        auto iteratorPairEdges = divideVector(edges, threadNum);

        std::unordered_set<PageId, PageIdHash> danglingNodes;
        getDanglingNodes(iteratorPairPages, danglingNodes);

        while (nextIteration) {
            pageRankMap rankMap;
            double localDangleSum = 0;

            for (auto danglingNode : danglingNodes) {
                localDangleSum += pageHashMap[danglingNode];
            }

            if (localDangleSum != 0) {
                // Add dangle sum to global counter
                std::unique_lock<std::mutex> mutex_lock(mutex);
                dangleSum += localDangleSum;
            }

            for (auto currentEdge = iteratorPairEdges.first;
                 currentEdge != iteratorPairEdges.second; ++currentEdge) {

                // currentEdge->first - link source
                // currentEdge->second - link target
                // pageHashMap and numLinks read only in this section, thread safe
                rankMap[currentEdge->second] += alpha * pageHashMap[currentEdge->first] / numLinks[currentEdge->first];
            }

            addRankSums(rankMap);

            barrier.reach(); // Barrier 1

            double localDifference = getLocalDifference(iteratorPairPages, alpha, network.getSize());

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
