#include "trader/providers/nasdaq/itch_handler.h"
#include "market_manager_jasper.h"
#include "order_book_jasper.h"
#include <OptionParser.h>
#include "benchmark/reporter_console.h"
#include "filesystem/file.h"
#include "system/stream.h"
#include "time/timestamp.h"

using namespace CppCommon;
using namespace CppTrader;
using namespace CppTrader::ITCH;

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
