#ifndef ORDER_BOOK_JASPER_H
#define ORDER_BOOK_JASPER_H
#include <cstdint>
#include <cstring>
#include "containers/list.h"
#include "containers/bintree_avl.h"
class MarketManagerJapser;
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

    void addOrder(OrderNode* order) {
        this->TotalVolume += order->Quantity;
        OrderList.push_back(*order);
    }

    void reduceOrder(OrderNode* order, uint32_t quantity) {
        order->Quantity -= quantity;
        this->TotalVolume -= quantity;
        if (order->Quantity == 0)
            OrderList.pop_current(*order);
    }
    void deleteOrder(OrderNode* order) {
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

    OrderBook(MarketManagerJapser& manager) :
        _manager(manager)
    {
    }
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
    MarketManagerJapser& _manager;
    LevelNode* _best_bid;
    LevelNode* _best_ask;
    Levels _bids;
    Levels _asks;
    typedef std::unordered_map<uint64_t, OrderNode*> Orders;
    Orders _orders;

    std::pair<LevelNode*, UpdateType> FindLevel(OrderNode* order_ptr);

    LevelNode* GetLevel(OrderNode* order_ptr)
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

    void DeleteLevel(OrderNode* order_ptr);

    LevelUpdate AddOrder(OrderNode* order_ptr)
    {
        // Find the price level for the order
        std::pair<LevelNode*, UpdateType> find_result = FindLevel(order_ptr);
        auto level_ptr = find_result.first;
        level_ptr->addOrder(order_ptr);
        order_ptr->Level = level_ptr;
        // Price level was changed. Return top of the book modification flag.
        return LevelUpdate{find_result.second, level_ptr, level_ptr == ((order_ptr->Side == OrderSide::BUY) ? best_bid() : best_ask())};
    }

    LevelUpdate ReduceOrder(OrderNode* order_ptr, uint32_t quantity)
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

    LevelUpdate DeleteOrder(OrderNode* order_ptr)
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

#endif
