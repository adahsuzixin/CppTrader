#include "order_book_jasper.h"
#include "market_manager_jasper.h"
#include <iostream>
#include <utility>

std::pair<LevelNode*, UpdateType> OrderBook::FindLevel(OrderNode* order_ptr)
{
    if (order_ptr->Side == OrderSide::BUY)
    {
        // Try to find required price level in the bid collections
        auto it = _bids.find(LevelNode(LevelType::BID, order_ptr->Price));
        if (it != _bids.end()) 
            return std::make_pair(it.operator->(), UpdateType::UPDATE);

        // Create a new price level
        LevelNode *level_ptr = _manager._level_pool.Create(LevelType::BID, order_ptr->Price);
        _bids.insert(*level_ptr);

        // Update the best bid price level
        if ((_best_bid == nullptr) || (level_ptr->Price > _best_bid->Price))
            _best_bid = level_ptr;
        return std::make_pair(level_ptr, UpdateType::ADD);
    }
    else
    {
        auto it = _asks.find(LevelNode(LevelType::ASK, order_ptr->Price));
        if (it != _asks.end()) 
            return std::make_pair(it.operator->(), UpdateType::UPDATE);

        // Create a new price level
        LevelNode *level_ptr = _manager._level_pool.Create(LevelType::ASK, order_ptr->Price);
        _asks.insert(*level_ptr);

        // Update the best bid price level
        if ((_best_ask == nullptr) || (level_ptr->Price > _best_ask->Price))
            _best_ask = level_ptr;

        return std::make_pair(level_ptr, UpdateType::ADD);
    }
}


void OrderBook::DeleteLevel(OrderNode* order_ptr)
{
    LevelNode* level_ptr = order_ptr->Level;
    if (order_ptr->Side == OrderSide::BUY)
    {
        // Update the best bid price level
        // std::cout << "before delete bid: " << _bids.size() << std::endl;
        if (level_ptr == _best_bid)
            _best_bid = (_best_bid->left != nullptr) ? _best_bid->left : ((_best_bid->parent != nullptr) ? _best_bid->parent : _best_bid->right);

        // Erase the price level from the bid collection
        _bids.erase(Levels::iterator(&_bids, level_ptr));
        // std::cout << "after delete bid: " << _bids.size() << std::endl;
    }
    else
    {
        // Update the best ask price level
        // std::cout << "before delete ask: " << _asks.size() << std::endl;
        if (level_ptr == _best_ask)
            _best_ask = (_best_ask->right != nullptr) ? _best_ask->right : ((_best_ask->parent != nullptr) ? _best_ask->parent : _best_ask->left);

        // Erase the price level from the ask collection
        _asks.erase(Levels::iterator(&_asks, level_ptr));
        // std::cout << "after delete ask: " << _asks.size() << std::endl;
    }
    _manager._level_pool.Release(level_ptr);
}

