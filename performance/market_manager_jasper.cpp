//
// Created by Ivan Shynkarenka on 11.08.2017
//

#include "trader/providers/nasdaq/itch_handler.h"

#include "benchmark/reporter_console.h"
#include "filesystem/file.h"
#include "system/stream.h"
#include "time/timestamp.h"

#include <OptionParser.h>

#include <algorithm>
#include <vector>
#include <unordered_map>
#include <memory>
#include <queue>
#include <list>
#include "containers/bintree_avl.h"
#include "containers/list.h"
#include "memory/allocator_pool.h"

using namespace CppCommon;
using namespace CppTrader;
using namespace CppTrader::ITCH;

enum class OrderSide : uint8_t
{
    BUY,
    SELL
};

struct Order
{
    Order(uint64_t id)
        : Id(id)
    {
    }
    uint64_t Id;
    uint16_t Symbol;
    OrderSide Side;
    uint32_t Price;
    uint32_t Quantity;
    std::list<std::shared_ptr<Order>>::iterator Position;
};

struct Symbol
{
    uint16_t Id;
    char Name[8];

    Symbol() noexcept : Id(0)
    {
        std::memset(Name, 0, sizeof(Name));
    }
    Symbol(uint16_t id, const char name[8]) noexcept : Id(id)
    {
        std::memcpy(Name, name, sizeof(Name));
    }
};

enum class LevelType : uint8_t
{
    BID,
    ASK
};

// struct Level
// {
//     LevelType Type;
//     uint32_t Price;
//     uint32_t Volume;
//     size_t Orders;
// };

enum class UpdateType : uint8_t
{
    ADD,
    UPDATE,
    DELETE
};

struct LevelNode;

struct OrderNode : public Order, public CppCommon::List<OrderNode>::Node
{
    LevelNode* Level;
    OrderNode(uint64_t id) noexcept;
    OrderNode(const Order& order) noexcept;
    OrderNode(const OrderNode&) noexcept = default;
    OrderNode(OrderNode&&) noexcept = default;
    ~OrderNode() noexcept = default;

    OrderNode& operator=(const Order& order) noexcept;
    OrderNode& operator=(const OrderNode&) noexcept = default;
    OrderNode& operator=(OrderNode&&) noexcept = default;
};

inline OrderNode::OrderNode(const Order& order) noexcept : Order(order), Level(nullptr)
{
}

inline OrderNode::OrderNode(uint64_t id) noexcept : Order(id), Level(nullptr)
{
}

inline OrderNode& OrderNode::operator=(const Order& order) noexcept
{
    Order::operator=(order);
    Level = nullptr;
    return *this;
}

//! Price level
struct Level
{
    //! Level type
    LevelType Type;
    //! Level price
    uint64_t Price;
    //! Level volume
    uint64_t TotalVolume;
    //! Level hidden volume
    uint64_t HiddenVolume;
    //! Level visible volume
    uint64_t VisibleVolume;
    //! Level orders
    size_t Orders;

    Level(LevelType type, uint64_t price) noexcept;
    Level(const Level&) noexcept = default;
    Level(Level&&) noexcept = default;
    ~Level() noexcept = default;

    Level& operator=(const Level&) noexcept = default;
    Level& operator=(Level&&) noexcept = default;

    // Price level comparison
    friend bool operator==(const Level& level1, const Level& level2) noexcept
    { return level1.Price == level2.Price; }
    friend bool operator!=(const Level& level1, const Level& level2) noexcept
    { return level1.Price != level2.Price; }
    friend bool operator<(const Level& level1, const Level& level2) noexcept
    { return level1.Price < level2.Price; }
    friend bool operator>(const Level& level1, const Level& level2) noexcept
    { return level1.Price > level2.Price; }
    friend bool operator<=(const Level& level1, const Level& level2) noexcept
    { return level1.Price <= level2.Price; }
    friend bool operator>=(const Level& level1, const Level& level2) noexcept
    { return level1.Price >= level2.Price; }

    template <class TOutputStream>
    friend TOutputStream& operator<<(TOutputStream& stream, const Level& level);

    //! Is the bid price level?
    bool IsBid() const noexcept { return Type == LevelType::BID; }
    //! Is the ask price level?
    bool IsAsk() const noexcept { return Type == LevelType::ASK; }
};

//! Price level node
struct LevelNode : public Level, public CppCommon::BinTreeAVL<LevelNode>::Node
{
    //! Price level orders
    CppCommon::List<OrderNode> OrderList;

    LevelNode(LevelType type, uint64_t price) noexcept;
    LevelNode(const Level& level) noexcept;
    LevelNode(const LevelNode&) noexcept = default;
    LevelNode(LevelNode&&) noexcept = default;
    ~LevelNode() noexcept = default;

    LevelNode& operator=(const Level& level) noexcept;
    LevelNode& operator=(const LevelNode&) noexcept = default;
    LevelNode& operator=(LevelNode&&) noexcept = default;

    // Price level comparison
    friend bool operator==(const LevelNode& level1, const LevelNode& level2) noexcept
    { return level1.Price == level2.Price; }
    friend bool operator!=(const LevelNode& level1, const LevelNode& level2) noexcept
    { return level1.Price != level2.Price; }
    friend bool operator<(const LevelNode& level1, const LevelNode& level2) noexcept
    { return level1.Price < level2.Price; }
    friend bool operator>(const LevelNode& level1, const LevelNode& level2) noexcept
    { return level1.Price > level2.Price; }
    friend bool operator<=(const LevelNode& level1, const LevelNode& level2) noexcept
    { return level1.Price <= level2.Price; }
    friend bool operator>=(const LevelNode& level1, const LevelNode& level2) noexcept
    { return level1.Price >= level2.Price; }

    void addOrder(std::shared_ptr<OrderNode> order) {
        this->TotalVolume += order->Quantity;
        OrderList.push_back(*order);
    }

    void reduceOrder(std::shared_ptr<OrderNode> order, uint32_t quantity) {
        order->Quantity -= quantity;
        this->TotalVolume -= quantity;
        if (order->Quantity == 0)
            OrderList.pop_current(*order);
    }
    void deleteOrder(std::shared_ptr<OrderNode> order) {
        this->TotalVolume -= order->Quantity;
        OrderList.pop_current(*order);
    }
};


inline LevelNode::LevelNode(LevelType type, uint64_t price) noexcept
    : Level(type, price)
{
}

inline LevelNode::LevelNode(const Level& level) noexcept : Level(level)
{
}

inline Level::Level(LevelType type, uint64_t price) noexcept
    : Type(type),
      Price(price),
      TotalVolume(0),
      HiddenVolume(0),
      VisibleVolume(0),
      Orders(0)
{
}


struct LevelUpdate
{
    UpdateType Type;
    LevelNode* Update;
    bool Top;
};

class OrderBook
{
    friend class MarketManagerJapser;

public:
    typedef CppCommon::BinTreeAVL<LevelNode, std::less<LevelNode>> Levels;
    // typedef std::map<uint32_t, std::shared_ptr<PriceLevel>> Levels;

    OrderBook() = default;
    OrderBook(const OrderBook &) = delete;
    OrderBook(OrderBook &&) noexcept = default;
    ~OrderBook()
    {
    }

    OrderBook &operator=(const OrderBook &) = delete;
    OrderBook &operator=(OrderBook &&) noexcept = default;

    explicit operator bool() const noexcept { return !empty(); }

    bool empty() const noexcept { return _bids.empty() && _asks.empty(); }
    size_t size() const noexcept { return _bids.size() + _asks.size(); }
    const Levels &bids() const noexcept { return _bids; }
    const Levels &asks() const noexcept { return _asks; }
    //! Get the order book best bid price level
    const LevelNode* best_bid() const noexcept { return _best_bid; }
    //! Get the order book best ask price level
    const LevelNode* best_ask() const noexcept { return _best_ask; }
private:
    LevelNode* _best_bid;
    LevelNode* _best_ask;
    Levels _bids;
    Levels _asks;
    CppCommon::PoolMemoryManager<CppCommon::DefaultMemoryManager> _order_memory_manager;
    CppCommon::PoolAllocator<OrderNode, CppCommon::DefaultMemoryManager> _order_pool;
    typedef std::unordered_map<uint64_t, OrderNode*> Orders;
    Orders _orders;

    std::pair<LevelNode*, UpdateType> FindLevel(std::shared_ptr <OrderNode> order_ptr)
    {
        if (order_ptr->Side == OrderSide::BUY)
        {
            // Try to find required price level in the bid collections
            auto it = _bids.find(LevelNode(LevelType::BID, order_ptr->Price));
            if (it != _bids.end()) 
                return std::make_pair(it.operator->(), UpdateType::UPDATE);

            // Create a new price level
            LevelNode *level_ptr = new LevelNode(LevelType::BID, order_ptr->Price);
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
            LevelNode *level_ptr = new LevelNode(LevelType::ASK, order_ptr->Price);
            _asks.insert(*level_ptr);

            // Update the best bid price level
            if ((_best_ask == nullptr) || (level_ptr->Price > _best_ask->Price))
                _best_ask = level_ptr;

            return std::make_pair(level_ptr, UpdateType::ADD);
        }
    }

    LevelNode* GetLevel(std::shared_ptr <OrderNode> order_ptr)
    {
        if (order_ptr->Side == OrderSide::BUY)
        {
            auto it = _bids.find(LevelNode(LevelType::BID, order_ptr->Price));
            return (it != _bids.end()) ? it.operator->() : nullptr;
        }
        else
        {
            // Try to find required price level in the bid collections
            auto it = _asks.find(LevelNode(LevelType::ASK, order_ptr->Price));
            return (it != _asks.end()) ? it.operator->() : nullptr;
        }
    }

    void DeleteLevel(std::shared_ptr <OrderNode> order_ptr)
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
        delete level_ptr;
    }

    LevelUpdate AddOrder(std::shared_ptr< OrderNode > order_ptr)
    {
        // Find the price level for the order
        std::pair<LevelNode*, UpdateType> find_result = FindLevel(order_ptr);
        auto level_ptr = find_result.first;
        level_ptr->addOrder(order_ptr);
        order_ptr->Level = level_ptr;
        // Price level was changed. Return top of the book modification flag.
        return LevelUpdate{find_result.second, level_ptr, level_ptr == ((order_ptr->Side == OrderSide::BUY) ? best_bid() : best_ask())};
    }

    LevelUpdate ReduceOrder(std::shared_ptr <OrderNode> order_ptr, uint32_t quantity)
    {
        // Find the price level for the order
        LevelNode* level_ptr = GetLevel(order_ptr);
        level_ptr->reduceOrder(order_ptr, quantity);

        LevelUpdate update = {UpdateType::UPDATE, level_ptr, level_ptr == ((order_ptr->Side == OrderSide::BUY) ? best_bid() : best_ask())};

        // Delete the empty price level
        if (level_ptr->TotalVolume == 0)
        {
            DeleteLevel(order_ptr);
            update.Type = UpdateType::DELETE;
        }
        if (order_ptr->Quantity == 0)
        {
            this->_orders.erase(order_ptr->Id);
        }

        return update;
    }

    LevelUpdate DeleteOrder(std::shared_ptr <OrderNode> order_ptr)
    {
        // Find the price level for the order
        LevelNode* level_ptr = GetLevel(order_ptr);
        level_ptr->deleteOrder(order_ptr);
        this->_orders.erase(order_ptr->Id);

        LevelUpdate update = {UpdateType::UPDATE, level_ptr, (level_ptr == ((order_ptr->Side == OrderSide::BUY) ? best_bid() : best_ask()))};

        // Delete the empty price level
        if (level_ptr->TotalVolume == 0)
        {
            DeleteLevel(order_ptr);
            update.Type = UpdateType::DELETE;
        }
        this->_orders.erase(order_ptr->Id);

        return update;
    }
};


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
public:
    explicit MarketManagerJapser(MarketHandler &market_handler) : _market_handler(market_handler)
    {
        _symbols.resize(10000);
        _order_books.resize(10000);
    }
    MarketManagerJapser(const MarketManagerJapser &) = delete;
    MarketManagerJapser(MarketManagerJapser &&) = delete;

    MarketManagerJapser &operator=(const MarketManagerJapser &) = delete;
    MarketManagerJapser &operator=(MarketManagerJapser &&) = delete;

    const Symbol *GetSymbol(uint16_t id) const noexcept { return &_symbols[id]; }
    const OrderBook *GetOrderBook(uint16_t id) const noexcept { return &_order_books[id]; }

    std::shared_ptr <OrderNode> GetOrderExcept(OrderBook* order_book, uint64_t id)
    {
        auto it = order_book->_orders.find(id);
        if (it == order_book->_orders.end()) {
            std::shared_ptr <OrderNode> ret = std::make_shared<OrderNode>(id);
            order_book->_orders[id] = ret;
            return ret;
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
        _order_books[symbol.Id] = OrderBook();

        // Call the corresponding handler
        _market_handler.onAddOrderBook(_order_books[symbol.Id]);
    }

    void DeleteOrderBook(uint32_t id)
    {
        // Call the corresponding handler
        _market_handler.onDeleteOrderBook(_order_books[id]);
    }

    void AddOrder(uint64_t id, uint16_t symbol, OrderSide side, uint32_t price, uint32_t quantity)
    {
        // Add the new order into the order book
        OrderBook *order_book_ptr = &_order_books[symbol];
        // Insert the order
        std::shared_ptr<OrderNode> order_ptr = GetOrderExcept(order_book_ptr, id);
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
        OrderBook *order_book_ptr = &_order_books[symbol];
        std::shared_ptr<OrderNode> order_ptr = GetOrderExcept(order_book_ptr, id);

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
        OrderBook *order_book_ptr = &_order_books[symbol];
        // Get the order to modify
        std::shared_ptr <OrderNode> order_ptr = GetOrderExcept(order_book_ptr, id);

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
        OrderBook *order_book_ptr = &_order_books[symbol];
        // Get the order to replace
        std::shared_ptr <OrderNode> order_ptr = GetOrderExcept(order_book_ptr, id);

        UpdateLevel(*order_book_ptr, order_book_ptr->DeleteOrder(order_ptr));

        // Call the corresponding handler
        _market_handler.onDeleteOrder(*order_ptr);

        if (new_quantity > 0)
        {
            // Replace the order
            std::shared_ptr <OrderNode> new_order_ptr = GetOrderExcept(order_book_ptr, new_id);
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
        OrderBook *order_book_ptr = &_order_books[symbol];
        // Get the order to delete
        std::shared_ptr <OrderNode> order_ptr = GetOrderExcept(order_book_ptr, id);

        UpdateLevel(*order_book_ptr, order_book_ptr->DeleteOrder(order_ptr));

        // Call the corresponding handler
        _market_handler.onDeleteOrder(*order_ptr);
    }

    void ExecuteOrder(uint64_t id, uint16_t symbol, uint32_t quantity)
    {
        OrderBook *order_book_ptr = &_order_books[symbol];
        // Get the order to execute
        std::shared_ptr <OrderNode> order_ptr = GetOrderExcept(order_book_ptr, id);

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
        OrderBook *order_book_ptr = &_order_books[symbol];
        // Get the order to execute
        std::shared_ptr <OrderNode> order_ptr = GetOrderExcept(order_book_ptr, id);

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
    std::vector<OrderBook> _order_books;

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

class MyITCHHandler : public ITCHHandler
{
public:
    explicit MyITCHHandler(MarketManagerJapser &market)
        : _market(market),
          _messages(0),
          _errors(0)
    {
    }

    size_t messages() const { return _messages; }
    size_t errors() const { return _errors; }

protected:
    bool filterSymbol(uint16_t symbol) { 
        if (symbol != 381) {
            return true;
        }
        return false;
    }
    bool onMessage(const SystemEventMessage &message) override
    {
        // if (filterSymbol(message.StockLocate)) return true;
        ++_messages;
        return true;
    }
    bool onMessage(const StockDirectoryMessage &message) override
    {
        // if (filterSymbol(message.StockLocate)) return true;
        ++_messages;
        Symbol symbol(message.StockLocate, message.Stock);
        _market.AddSymbol(symbol);
        _market.AddOrderBook(symbol);
        return true;
    }
    bool onMessage(const StockTradingActionMessage &message) override
    {
        ++_messages;
        return true;
    }
    bool onMessage(const RegSHOMessage &message) override
    {
        ++_messages;
        return true;
    }
    bool onMessage(const MarketParticipantPositionMessage &message) override
    {
        ++_messages;
        return true;
    }
    bool onMessage(const MWCBDeclineMessage &message) override
    {
        ++_messages;
        return true;
    }
    bool onMessage(const MWCBStatusMessage &message) override
    {
        ++_messages;
        return true;
    }
    bool onMessage(const IPOQuotingMessage &message) override
    {
        ++_messages;
        return true;
    }
    bool onMessage(const AddOrderMessage &message) override
    {
        // if (filterSymbol(message.StockLocate)) return true;
        ++_messages;
        _market.AddOrder(message.OrderReferenceNumber, message.StockLocate, (message.BuySellIndicator == 'B') ? OrderSide::BUY : OrderSide::SELL, message.Price, message.Shares);
        return true;
    }
    bool onMessage(const AddOrderMPIDMessage &message) override
    {
        // if (filterSymbol(message.StockLocate)) return true;
        ++_messages;
        _market.AddOrder(message.OrderReferenceNumber, message.StockLocate, (message.BuySellIndicator == 'B') ? OrderSide::BUY : OrderSide::SELL, message.Price, message.Shares);
        return true;
    }
    bool onMessage(const OrderExecutedMessage &message) override
    {
        // if (filterSymbol(message.StockLocate)) return true;
        ++_messages;
        _market.ExecuteOrder(message.OrderReferenceNumber, message.StockLocate, message.ExecutedShares);
        return true;
    }
    bool onMessage(const OrderExecutedWithPriceMessage &message) override
    {
        // if (filterSymbol(message.StockLocate)) return true;
        ++_messages;
        _market.ExecuteOrder(message.OrderReferenceNumber, message.StockLocate, message.ExecutionPrice, message.ExecutedShares);
        return true;
    }
    bool onMessage(const OrderCancelMessage &message) override
    {
        // if (filterSymbol(message.StockLocate)) return true;
        ++_messages;
        _market.ReduceOrder(message.OrderReferenceNumber, message.StockLocate, message.CanceledShares);
        return true;
    }
    bool onMessage(const OrderDeleteMessage &message) override
    {
        // if (filterSymbol(message.StockLocate)) return true;
        ++_messages;
        _market.DeleteOrder(message.OrderReferenceNumber, message.StockLocate);
        return true;
    }
    bool onMessage(const OrderReplaceMessage &message) override
    {
        // if (filterSymbol(message.StockLocate)) return true;
        ++_messages;
        _market.ReplaceOrder(message.OriginalOrderReferenceNumber,
                             message.StockLocate,
                             message.NewOrderReferenceNumber,
                             message.Price,
                             message.Shares);
        return true;
    }
    bool onMessage(const TradeMessage &message) override
    {
        ++_messages;
        return true;
    }
    bool onMessage(const CrossTradeMessage &message) override
    {
        ++_messages;
        return true;
    }
    bool onMessage(const BrokenTradeMessage &message) override
    {
        ++_messages;
        return true;
    }
    bool onMessage(const NOIIMessage &message) override
    {
        ++_messages;
        return true;
    }
    bool onMessage(const RPIIMessage &message) override
    {
        ++_messages;
        return true;
    }
    bool onMessage(const LULDAuctionCollarMessage &message) override
    {
        ++_messages;
        return true;
    }
    bool onMessage(const UnknownMessage &message) override
    {
        ++_errors;
        return true;
    }

private:
    MarketManagerJapser &_market;
    size_t _messages;
    size_t _errors;
};

int main(int argc, char **argv)
{
    auto parser = optparse::OptionParser().version("1.0.0.0");

    parser.add_option("-i", "--input").dest("input").help("Input file name");

    optparse::Values options = parser.parse_args(argc, argv);

    // Print help
    if (options.get("help"))
    {
        parser.print_help();
        return 0;
    }

    MarketHandler market_handler;
    MarketManagerJapser market(market_handler);
    MyITCHHandler itch_handler(market);

    // Open the input file or stdin
    std::unique_ptr<Reader> input(new StdInput());
    if (options.is_set("input"))
    {
        File *file = new File(Path(options.get("input")));
        file->Open(true, false);
        input.reset(file);
    }

    // Perform input
    size_t size;
    uint8_t buffer[8192];
    std::cout << "ITCH processing...";
    uint64_t timestamp_start = Timestamp::nano();
    while ((size = input->Read(buffer, sizeof(buffer))) > 0)
    {
        // Process the buffer
        itch_handler.Process(buffer, size);
    }
    uint64_t timestamp_stop = Timestamp::nano();
    std::cout << "Done!" << std::endl;

    std::cout << std::endl;

    std::cout << "Errors: " << itch_handler.errors() << std::endl;

    std::cout << std::endl;

    size_t total_messages = itch_handler.messages();
    size_t total_updates = market_handler.updates();

    std::cout << "Processing time: " << CppBenchmark::ReporterConsole::GenerateTimePeriod(timestamp_stop - timestamp_start) << std::endl;
    std::cout << "Total ITCH messages: " << total_messages << std::endl;
    std::cout << "ITCH message latency: " << CppBenchmark::ReporterConsole::GenerateTimePeriod((timestamp_stop - timestamp_start) / total_messages) << std::endl;
    std::cout << "ITCH message throughput: " << total_messages * 1000000000 / (timestamp_stop - timestamp_start) << " msg/s" << std::endl;
    std::cout << "Total market updates: " << total_updates << std::endl;
    std::cout << "Market update latency: " << CppBenchmark::ReporterConsole::GenerateTimePeriod((timestamp_stop - timestamp_start) / total_updates) << std::endl;
    std::cout << "Market update throughput: " << total_updates * 1000000000 / (timestamp_stop - timestamp_start) << " upd/s" << std::endl;

    std::cout << std::endl;

    std::cout << "Market statistics: " << std::endl;
    std::cout << "Max symbols: " << market_handler.max_symbols() << std::endl;
    std::cout << "Max order books: " << market_handler.max_order_books() << std::endl;
    std::cout << "Max order book levels: " << market_handler.max_order_book_levels() << std::endl;
    std::cout << "Max orders: " << market_handler.max_orders() << std::endl;

    std::cout << std::endl;

    std::cout << "Order statistics: " << std::endl;
    std::cout << "Add order operations: " << market_handler.add_orders() << std::endl;
    std::cout << "Update order operations: " << market_handler.update_orders() << std::endl;
    std::cout << "Delete order operations: " << market_handler.delete_orders() << std::endl;
    std::cout << "Execute order operations: " << market_handler.execute_orders() << std::endl;

    return 0;
}
