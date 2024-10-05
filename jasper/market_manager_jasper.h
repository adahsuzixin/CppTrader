#ifndef MARKET_MANAGER_JASPER_H
#define MARKET_MANAGER_JASPER_H

#include "trader/matching/market_handler.h"
#include "trader/providers/nasdaq/itch_handler.h"
#include "order_book_jasper.h"

class MarketHandler
{
    friend class MarketManagerJapser;

public:
    MarketHandler()
        : _updates(0),
          _symbols(0),
          _max_symbols(0),
          _order_books(0),
          _max_order_books(0),
          _max_order_book_levels(0),
          _orders(0),
          _max_orders(0),
          _add_orders(0),
          _update_orders(0),
          _delete_orders(0),
          _execute_orders(0)
    {
    }

    size_t updates() const { return _updates; }
    size_t max_symbols() const { return _max_symbols; }
    size_t max_order_books() const { return _max_order_books; }
    size_t max_order_book_levels() const { return _max_order_book_levels; }
    size_t max_orders() const { return _max_orders; }
    size_t add_orders() const { return _add_orders; }
    size_t update_orders() const { return _update_orders; }
    size_t delete_orders() const { return _delete_orders; }
    size_t execute_orders() const { return _execute_orders; }

protected:
    void onAddSymbol(const Symbol &symbol)
    {
        ++_updates;
        ++_symbols;
        _max_symbols = std::max(_symbols, _max_symbols);
    }
    void onDeleteSymbol(const Symbol &symbol)
    {
        ++_updates;
        --_symbols;
    }
    void onAddOrderBook(const OrderBook &order_book)
    {
        ++_updates;
        ++_order_books;
        _max_order_books = std::max(_order_books, _max_order_books);
    }
    void onUpdateOrderBook(const OrderBook &order_book, bool top, int symbol_id) {
        auto cur_max = std::max(order_book.bids().size(), order_book.asks().size());
        if (cur_max > _max_order_book_levels)
        {
            _max_order_book_levels = cur_max;
            _max_level_symbol = symbol_id;
            // std::cout << "Max level symbol: " << _max_level_symbol << " Max levels: " << _max_order_book_levels << std::endl;
        }
    }
    void onDeleteOrderBook(const OrderBook &order_book)
    {
        ++_updates;
        --_order_books;
    }
    void onAddLevel(const OrderBook &order_book, LevelNode* level, bool top) { ++_updates; }
    void onUpdateLevel(const OrderBook &order_book, LevelNode* level, bool top) { ++_updates; }
    void onDeleteLevel(const OrderBook &order_book, LevelNode* level, bool top) { ++_updates; }
    void onAddOrder(const Order &order)
    {
        ++_updates;
        ++_orders;
        _max_orders = std::max(_orders, _max_orders);
        ++_add_orders;
    }
    void onUpdateOrder(const Order &order)
    {
        ++_updates;
        ++_update_orders;
    }
    void onDeleteOrder(const Order &order)
    {
        ++_updates;
        --_orders;
        ++_delete_orders;
    }
    void onExecuteOrder(const Order &order, int64_t price, uint64_t quantity)
    {
        ++_updates;
        ++_execute_orders;
    }

private:
    size_t _updates;
    size_t _symbols;
    size_t _max_symbols;
    size_t _order_books;
    size_t _max_order_books;
    size_t _max_order_book_levels;
    size_t _max_level_symbol;
    size_t _orders;
    size_t _max_orders;
    size_t _add_orders;
    size_t _update_orders;
    size_t _delete_orders;
    size_t _execute_orders;
};

class MarketManagerJapser
{
    friend class OrderBook;
public:
    MarketManagerJapser(MarketHandler& market_handler)
    : _market_handler(market_handler),
      _auxiliary_memory_manager(),
      _order_book_memory_manager(_auxiliary_memory_manager),
      _order_book_pool(_order_book_memory_manager),
      _order_memory_manager(_auxiliary_memory_manager),
      _order_pool(_order_memory_manager),
      _level_memory_manager(_auxiliary_memory_manager),
      _level_pool(_level_memory_manager)
    {
        _symbols.resize(10000);
        _order_books.resize(10000);
    }   
    MarketManagerJapser(const MarketManagerJapser &) = delete;
    MarketManagerJapser(MarketManagerJapser &&) = delete;

    MarketManagerJapser &operator=(const MarketManagerJapser &) = delete;
    MarketManagerJapser &operator=(MarketManagerJapser &&) = delete;

    const Symbol *GetSymbol(uint16_t id) const noexcept { return &_symbols[id]; }
    const OrderBook *GetOrderBook(uint16_t id) const noexcept { return _order_books[id]; }

    OrderNode* GetOrderExcept(OrderBook* order_book, uint64_t id)
    {
        auto it = order_book->_orders.find(id);
        if (it == order_book->_orders.end()) {
            OrderNode* order_ptr = _order_pool.Create(id);
            order_book->_orders[id] = order_ptr;
            return order_ptr;
        } else {
            return it->second;
        }

    }

    void AddSymbol(const Symbol &symbol)
    {
        // Insert the symbol
        _symbols[symbol.Id] = symbol;

        // Call the corresponding handler
        _market_handler.onAddSymbol(_symbols[symbol.Id]);
    }

    void DeleteSymbol(uint32_t id)
    {
        // Call the corresponding handler
        _market_handler.onDeleteSymbol(_symbols[id]);
    }

    void AddOrderBook(const Symbol &symbol)
    {
        // Insert the order book
        OrderBook* order_book_ptr = _order_book_pool.Create(*this);
        _order_books[symbol.Id] = order_book_ptr;

        // Call the corresponding handler
        _market_handler.onAddOrderBook(*order_book_ptr);
    }

    void DeleteOrderBook(uint32_t id)
    {
        // Call the corresponding handler
        _market_handler.onDeleteOrderBook(*_order_books[id]);
    }

    void AddOrder(uint64_t id, uint16_t symbol, OrderSide side, uint32_t price, uint32_t quantity)
    {
        // Add the new order into the order book
        OrderBook *order_book_ptr = _order_books[symbol];
        // Insert the order
        OrderNode* order_ptr = GetOrderExcept(order_book_ptr, id);
        order_ptr->Symbol = symbol;
        order_ptr->Side = side;
        order_ptr->Quantity = quantity;
        order_ptr->Price = price;
        // Call the corresponding handler
        _market_handler.onAddOrder(*order_ptr);
        UpdateLevel(*order_book_ptr, order_book_ptr->AddOrder(order_ptr), order_ptr->Symbol);
    }

    void ReduceOrder(uint64_t id, uint16_t symbol, uint32_t quantity)
    {
        OrderBook *order_book_ptr = _order_books[symbol];
        OrderNode* order_ptr = GetOrderExcept(order_book_ptr, id);

        // Calculate the minimal possible order quantity to reduce
        auto left_quantity = order_ptr->Quantity;
        quantity = std::min(quantity, order_ptr->Quantity);

        // Reduce the order quantity
        left_quantity -= quantity;

        // Update the order or delete the empty order
        if (left_quantity > 0)
        {
            // Call the corresponding handler
            _market_handler.onUpdateOrder(*order_ptr);

            // Reduce the order in the order book
            UpdateLevel(*order_book_ptr, order_book_ptr->ReduceOrder(order_ptr, quantity));
        }
        else
        {
            // Call the corresponding handler
            _market_handler.onDeleteOrder(*order_ptr);

            // Delete the order in the order book
            UpdateLevel(*order_book_ptr, order_book_ptr->DeleteOrder(order_ptr));
        }
    }

    void ModifyOrder(uint64_t id, uint16_t symbol, int32_t new_price, uint32_t new_quantity)
    {
        OrderBook *order_book_ptr = _order_books[symbol];
        // Get the order to modify
        OrderNode* order_ptr = GetOrderExcept(order_book_ptr, id);

        // Delete the order from the order book
        UpdateLevel(*order_book_ptr, order_book_ptr->DeleteOrder(order_ptr));

        // Modify the order
        order_ptr->Price = new_price;
        order_ptr->Quantity = new_quantity;

        // Update the order or delete the empty order
        if (order_ptr->Quantity > 0)
        {
            // Call the corresponding handler
            _market_handler.onUpdateOrder(*order_ptr);

            // Add the modified order into the order book
            UpdateLevel(*order_book_ptr, order_book_ptr->AddOrder(order_ptr));
        }
        else
        {
            // Call the corresponding handler
            _market_handler.onDeleteOrder(*order_ptr);
        }
    }

    void ReplaceOrder(uint64_t id, uint16_t symbol, uint64_t new_id, int32_t new_price, uint32_t new_quantity)
    {
        // Delete the old order from the order book
        OrderBook *order_book_ptr = _order_books[symbol];
        // Get the order to replace
        OrderNode* order_ptr = GetOrderExcept(order_book_ptr, id);

        UpdateLevel(*order_book_ptr, order_book_ptr->DeleteOrder(order_ptr));

        // Call the corresponding handler
        _market_handler.onDeleteOrder(*order_ptr);

        if (new_quantity > 0)
        {
            // Replace the order
            OrderNode* new_order_ptr = GetOrderExcept(order_book_ptr, new_id);
            new_order_ptr->Id = new_id;
            new_order_ptr->Symbol = order_ptr->Symbol;
            new_order_ptr->Side = order_ptr->Side;
            new_order_ptr->Price = new_price;
            new_order_ptr->Quantity = new_quantity;

            // Call the corresponding handler
            _market_handler.onAddOrder(*new_order_ptr);

            // Add the modified order into the order book
            UpdateLevel(*order_book_ptr, order_book_ptr->AddOrder(new_order_ptr));
        }
    }

    void DeleteOrder(uint64_t id, uint16_t symbol)
    {
        // Delete the order from the order book
        OrderBook *order_book_ptr = _order_books[symbol];
        // Get the order to delete
        OrderNode* order_ptr = GetOrderExcept(order_book_ptr, id);

        UpdateLevel(*order_book_ptr, order_book_ptr->DeleteOrder(order_ptr));

        // Call the corresponding handler
        _market_handler.onDeleteOrder(*order_ptr);
    }

    void ExecuteOrder(uint64_t id, uint16_t symbol, uint32_t quantity)
    {
        OrderBook *order_book_ptr = _order_books[symbol];
        // Get the order to execute
        OrderNode* order_ptr = GetOrderExcept(order_book_ptr, id);

        // Calculate the minimal possible order quantity to execute
        quantity = std::min(quantity, order_ptr->Quantity);

        // Call the corresponding handler
        _market_handler.onExecuteOrder(*order_ptr, order_ptr->Price, quantity);

        // Reduce the order quantity
        auto left_quantity = order_ptr->Quantity;
        left_quantity -= quantity;

        // Reduce the order in the order book
        UpdateLevel(*order_book_ptr, order_book_ptr->ReduceOrder(order_ptr, quantity));

        // Update the order or delete the empty order
        if (left_quantity > 0)
        {
            // Call the corresponding handler
            _market_handler.onUpdateOrder(*order_ptr);
        }
        else
        {
            // Call the corresponding handler
            _market_handler.onDeleteOrder(*order_ptr);
        }
    }

    void ExecuteOrder(uint64_t id, uint16_t symbol, int32_t price, uint32_t quantity)
    {
        OrderBook *order_book_ptr = _order_books[symbol];
        // Get the order to execute
        OrderNode* order_ptr = GetOrderExcept(order_book_ptr, id);

        // Calculate the minimal possible order quantity to execute
        quantity = std::min(quantity, order_ptr->Quantity);

        // Call the corresponding handler
        _market_handler.onExecuteOrder(*order_ptr, price, quantity);

        // Reduce the order quantity
        auto left_quantity = order_ptr->Quantity;
        left_quantity -= quantity;

        // Reduce the order in the order book
        UpdateLevel(*order_book_ptr, order_book_ptr->ReduceOrder(order_ptr, quantity));

        // Update the order or delete the empty order
        if (left_quantity > 0)
        {
            // Call the corresponding handler
            _market_handler.onUpdateOrder(*order_ptr);
        }
        else
        {
            // Call the corresponding handler
            _market_handler.onDeleteOrder(*order_ptr);
        }
    }

private:
    MarketHandler &_market_handler;

    std::vector<Symbol> _symbols;
    // Auxiliary memory manager
    CppCommon::DefaultMemoryManager _auxiliary_memory_manager;

    CppCommon::PoolMemoryManager<CppCommon::DefaultMemoryManager> _order_book_memory_manager;
    CppCommon::PoolAllocator<OrderBook, CppCommon::DefaultMemoryManager> _order_book_pool;
    std::vector<OrderBook*> _order_books;

    // Orders
    CppCommon::PoolMemoryManager<CppCommon::DefaultMemoryManager> _order_memory_manager;
    CppCommon::PoolAllocator<OrderNode, CppCommon::DefaultMemoryManager> _order_pool;

    // Price level memory manager
    CppCommon::PoolMemoryManager<CppCommon::DefaultMemoryManager> _level_memory_manager;
    CppCommon::PoolAllocator<LevelNode, CppCommon::DefaultMemoryManager> _level_pool;

    void UpdateLevel(const OrderBook &order_book, const LevelUpdate &update, int symbol_id = 0) const
    {
        switch (update.Type)
        {
        case UpdateType::ADD:
            _market_handler.onAddLevel(order_book, update.Update, update.Top);
            break;
        case UpdateType::UPDATE:
            _market_handler.onUpdateLevel(order_book, update.Update, update.Top);
            break;
        case UpdateType::DELETE:
            _market_handler.onDeleteLevel(order_book, update.Update, update.Top);
            break;
        default:
            break;
        }
        _market_handler.onUpdateOrderBook(order_book, update.Top, symbol_id);
    }
};

#endif
