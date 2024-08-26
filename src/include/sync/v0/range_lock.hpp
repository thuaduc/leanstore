#pragma once

#include <atomic>
#include <cassert>
#include <climits>
#include <cstdlib>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <sstream>
#include <thread>
#include <vector>

#include "node.hpp"

namespace leanstore {

template <typename T, unsigned maxLevel>
class ConcurrentRangeLock {
   private:
    int randomLevel();

    bool findInsert(T start, T end, Node<T> **preds, Node<T> **succs);

    bool findExact(T start, T end, Node<T> **preds, Node<T> **succs);

    void findDelete(T start, T end);

   public:
    Node<T> *tail;
    Node<T> *head;

    ConcurrentRangeLock();

    bool tryLock(T start, T end);

    bool releaseLock(T start, T end);

};

template <typename T, unsigned maxLevel>
ConcurrentRangeLock<T, maxLevel>::ConcurrentRangeLock() {
    auto min = std::numeric_limits<T>::min();
    auto max = std::numeric_limits<T>::max();

    head = new Node<T>();
    tail = new Node<T>();

    tail->initialize(max, max, maxLevel);
    head->initializeHead(min, min, maxLevel, tail);

    srand(0);
}

template <typename T, unsigned maxLevel>
int ConcurrentRangeLock<T, maxLevel>::randomLevel() {
    unsigned level = 0;
    while (rand() % 2 && level < maxLevel) {
        level++;
    }
    return level;
}

template <typename T, unsigned maxLevel>
bool ConcurrentRangeLock<T, maxLevel>::findInsert(T start, T end,
                                                  Node<T> **preds,
                                                  Node<T> **succs) {
    bool marked[1] = {false};
    bool snip;
    Node<T> *pred;
    Node<T> *curr = nullptr;
    Node<T> *succ;

retry:
    while (true) {
        pred = head;
        for (int level = maxLevel; level >= 0; level--) {
            curr = pred->next[level]->getReference();

            while (start > curr->getStart()) {
                succ = curr->next[level]->get(marked);
                while (marked[0]) {
                    snip = pred->next[level]->compareAndSet(curr, succ, false,
                                                            false);

                    if (!snip) goto retry;

                    curr = pred->next[level]->getReference();
                    succ = curr->next[level]->get(marked);
                }
                if (start >= curr->getStart()) {
                    pred = curr;
                    curr = succ;
                } else {
                    break;
                }
            }

            preds[level] = pred;
            succs[level] = curr;
        }
        return (!(start > pred->getEnd() && end < curr->getStart()));
    }
}

template <typename T, unsigned maxLevel>
bool ConcurrentRangeLock<T, maxLevel>::findExact(T start, T end,
                                                 Node<T> **preds,
                                                 Node<T> **succs) {
    bool marked[1] = {false};
    bool snip;
    Node<T> *pred;
    Node<T> *curr = nullptr;
    Node<T> *succ;

retry:
    while (true) {
        pred = head;
        for (int level = maxLevel; level >= 0; level--) {
            curr = pred->next[level]->getReference();

            while (start >= curr->getStart()) {
                succ = curr->next[level]->get(marked);
                while (marked[0]) {
                    snip = pred->next[level]->compareAndSet(curr, succ, false,
                                                            false);

                    if (!snip) goto retry;

                    curr = pred->next[level]->getReference();
                    succ = curr->next[level]->get(marked);
                }
                if (start >= curr->getEnd()) {
                    pred = curr;
                    curr = succ;
                } else {
                    break;
                }
            }

            preds[level] = pred;
            succs[level] = curr;
        }
        return (start == curr->getStart() && end == curr->getEnd());
    }
}

template <typename T, unsigned maxLevel>
void ConcurrentRangeLock<T, maxLevel>::findDelete(T start, T end) {
    bool marked[1] = {false};
    bool snip;
    Node<T> *pred;
    Node<T> *curr = nullptr;
    Node<T> *succ;

retry:
    while (true) {
        pred = head;
        for (int level = maxLevel; level >= 0; level--) {
            curr = pred->next[level]->getReference();

            while (start >= curr->getStart()) {
                succ = curr->next[level]->get(marked);
                while (marked[0]) {
                    snip = pred->next[level]->compareAndSet(curr, succ, false,
                                                            false);

                    if (!snip) goto retry;

                    curr = pred->next[level]->getReference();
                    succ = curr->next[level]->get(marked);
                }
                if (start >= curr->getEnd()) {
                    pred = curr;
                    curr = succ;
                } else {
                    break;
                }
            }
        }
        return;
    }
}

template <typename T, unsigned maxLevel>
bool ConcurrentRangeLock<T, maxLevel>::tryLock(T start, T end) {
    int topLevel = randomLevel();
    Node<T> *preds[maxLevel + 1];
    Node<T> *succs[maxLevel + 1];

    while (true) {
        bool found = findInsert(start, end, preds, succs);
        if (found) {
            return false;
        } else {
            auto newNode = new Node<T>();
            newNode->initialize(start, end, topLevel);

            for (int level = 0; level <= topLevel; ++level) {
                Node<T> *succ = succs[level];
                newNode->next[level]->store(succ, false);
            }

            auto pred = preds[0];
            auto succ = succs[0];

            newNode->next[0]->store(succ, false);
            if (!pred->next[0]->compareAndSet(succ, newNode, false, false)) {
                continue;
            }

            for (int level = 1; level <= topLevel; ++level) {
                while (true) {
                    pred = preds[level];
                    succ = succs[level];
                    if (pred->next[level]->compareAndSet(succ, newNode, false,
                                                         false)) {
                        break;
                    } else {
                        findInsert(start, end, preds, succs);
                    }
                }
            }

            return true;
        }
    }
}

template <typename T, unsigned maxLevel>
bool ConcurrentRangeLock<T, maxLevel>::releaseLock(T start, T end) {
    Node<T> *preds[maxLevel + 1];
    Node<T> *succs[maxLevel + 1];
    Node<T> *succ;

    while (true) {
        bool found = findExact(start, end, preds, succs);
        if (!found) {
            std::cerr << "Range not found. Wrong usage of releaseLock. "
                      << start << " " << end << ". succ[0]"
                      << succs[0]->getStart() << " " << succs[0]->getEnd()
                      << std::endl;
            return false;
        } else {
            Node<T> *nodeToRemove = succs[0];
            for (int level = nodeToRemove->getTopLevel(); level >= 0 + 1;
                 level--) {
                bool marked[1] = {false};
                succ = nodeToRemove->next[level]->get(marked);
                while (!marked[0]) {
                    nodeToRemove->next[level]->attemptMark(succ, true);
                    succ = nodeToRemove->next[level]->get(marked);
                }
            }

            bool marked[1] = {false};
            succ = nodeToRemove->next[0]->get(marked);
            while (true) {
                bool iMarkedIt = nodeToRemove->next[0]->compareAndSet(
                    succ, succ, false, true);
                succ = succs[0]->next[0]->get(marked);
                if (iMarkedIt) {
                    findDelete(start, end);

                    return true;
                } else if (marked[0]) {
                    std::cerr << "Other thread is trying to release this "
                                 "range. Wrong usage of releaseLock somewhere."
                              << std::endl;
                    return false;
                }
            }
        }
    }
}

}