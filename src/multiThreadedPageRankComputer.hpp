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

    using pageIteratorPair = std::pair<std::vector<Page>::const_iterator, std::vector<Page>::const_iterator>;

    /*
     * Reusable barrier, waits for resistance number of threads
     */
    class Barrier {
    public:
        explicit Barrier(uint32_t resistance) : resistance(resistance), waiting(0), generation(0) {}

        void reach() {
            std::unique_lock<std::mutex> mutex_lock{cv_mutex};
            std::size_t current_generation = generation;

            if (++waiting < resistance) {
                // If current generation is different than remembered generation, all threads have reach the barrier
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

    uint32_t numThreads; // Number of threads in the pool

    mutable pageRankMap pageHashMap; // Stores rank for each page from last iteration
    mutable pageRankMap currentPageHashMap; // Stores rank for each page from current iteration
    mutable std::unordered_map<PageId, uint32_t, PageIdHash> numLinks; // Stores number of links outgoing from each page

    mutable Barrier barrier; // Synchronizes threads
    mutable std::mutex mutex; // Used for writing to Computer attributes

    mutable bool nextIteration; // True if PageRank should continue with next iteration of algorithm
    mutable double dangleSum; // Sum of ranks of dangling nodes in a given iteration
    mutable double difference; // Sum of differences between new and old ranks for each page

    /*
     * Resets computer attributes before every computation
     */
    void resetComputer() const {
        pageHashMap = pageRankMap();
        currentPageHashMap = pageRankMap();
        numLinks = std::unordered_map<PageId, uint32_t, PageIdHash>();
        dangleSum = 0;
        difference = 0;
        nextIteration = true;
    }

    /*
     * Divides vector into equal parts for each thread, returns iterators to first and one after the last
     * elements of the part for given thread
     */
    template<typename T>
    std::pair<typename std::vector<T>::const_iterator, typename std::vector<T>::const_iterator>
    divideVector(std::vector<T> const& vector, uint32_t threadNum) const {
        size_t sizePerThread = vector.size() / numThreads;
        auto beginning = vector.begin() + threadNum * sizePerThread;
        auto end = beginning + sizePerThread;
        if (threadNum == numThreads - 1) {
            end = vector.end();
        }
        return std::make_pair(beginning, end);
    }

    /*
     * Creates threads that generate Ids for each page
     */
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

    /*
     * Function used by threads in the pool to generate Ids for their pages
     */
    void generateIdsThread(uint32_t threadNum, Network const& network) const {
        std::vector<Page> const& pages = network.getPages();

        pageIteratorPair iteratorPair = divideVector(pages, threadNum);

        for (auto currentPage = iteratorPair.first; currentPage != iteratorPair.second; ++currentPage)
            currentPage->generateId(network.getGenerator());
    }

    /*
     * Inserts all edges from network into edgesVectors, initializes pageHashMap, currentPageHashMap
     * and numLinks
     */
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

    /*
     * Inserts dangling nodes between iteratorPairPages.first and iteratorPairPages.second into
     * the given set
     */
    void getDanglingNodes(pageIteratorPair iteratorPairPages,
                          std::unordered_set<PageId, PageIdHash>& danglingNodes) const {
        for (auto currentPage = iteratorPairPages.first;
             currentPage != iteratorPairPages.second; ++currentPage) {
            if (numLinks[currentPage->getId()] == 0) { // Read only, thread safe
                danglingNodes.insert(currentPage->getId());
            }
        }
    }

    /*
     * Adds ranks of nodes from rankMap to global map of current ranks
     */
    void addRankSums(pageRankMap const& rankMap) const {
        std::unique_lock<std::mutex> mutex_lock(mutex);
        for (auto idRank : rankMap) {
            PageId pageId = idRank.first;
            PageRank rank = idRank.second;

            currentPageHashMap[pageId] += rank;
        }
    }

    /*
     * Returns sum of new and old ranks differences of nodes between iteratorPairPages.first and
     * iteratorPairPages.second, sets the old rank to the current rank and current rank to 0
     */
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

    /*
     * Function used by threads in the pool, manages iterations of PageRank algorithm
     */
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

            for (PageId const& danglingNode : danglingNodes) {
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
                rankMap[currentEdge->second] +=
                        alpha * pageHashMap[currentEdge->first] / numLinks[currentEdge->first];
            }

            addRankSums(rankMap);

            // Difference between old and new ranks can be calculated only after all threads have
            // finished calculating the new ranks
            barrier.reach(); // Barrier 1

            double localDifference = getLocalDifference(iteratorPairPages, alpha, network.getSize());

            { // Add local difference to global counter
                std::unique_lock<std::mutex> mutex_lock(mutex);
                difference += localDifference;
            }

            // Main thread can start updating nextIteration after all threads reach barrier 2
            barrier.reach(); // Barrier 2

            // Waits for nextIteration update
            barrier.reach(); // Barrier 3
        }

    }
};

#endif /* SRC_MULTITHREADEDPAGERANKCOMPUTER_HPP_ */
