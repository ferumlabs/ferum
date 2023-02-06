// Ferum Architecture highlights.
//
// Ferum Price Store:
//
//        -\                                                                       /-
//          -\                                                                   /-
//            --   +-------------------------+    +------------------------+   --
// Buy Tree        |       Buy Cache         |    |       Sell Cache       |         Sell Tree
//            /-   +-------------------------+    +------------------------+   -\
//          /-                                                                   -\
//        /-                                                                       -\
//
// Ferum's orderbook for a single side is dvided into two main components, a vector cache and a B+ tree.
// Both the cache and the tree store a PriceStoreElem, which encapsulates high level information about the pricelevel:
// qty, pendingMakerCrankQty, and the id of the PriceLevel object (which stores the actual order information for that
// pricelevel).
//
// New pricelevels are added to the cache until it is reaches capacity, at which point prices are added to the tree.
// When adding a new price, the range covered by the cache and the tree is noted and takes precendence. For example,
// say the cache is full with prices 1, 2, 3, and 4. A price of 3.5 will still be added to the cache. The same
// principle applies to the prices stored in the tree. Prices between the tree an the cache are rebalanced periodically
// via cranks (see `rebalance` entry method).
//
// The cache and tree are each stored as seperate objects. Ferum maintains a MarketSummary object with properies like
// max/min cache prices. This allows us to only load either the cache or the tree - optimizing storage read/write costs.
//
// Pending quantities:
//
// To avoid having to load multiple different objects from storage, Ferum maintains pending quantities for executions.
// Each pricelevel maintains a pending maker quantity indicating how much quantity in that price level has already been
// executed as a maker and each order has a takerCrankPendingQty property. Pending quantities are "resolved" and
// settled as executions via the crank (see `crank` method). This allows Ferum to handle execution via a modification
// of a single value vs having to load and write multiple different objects.
//
// Object Reuse:
//
// Ferum reuses objects to avoid having to pay the expensive object create cost. Objects are created with unique IDs
// and, when they are no longer used, are added into an unused stack. When creating a new object, the stack is first
// checked to see if we can use an existing object. See OrderReuseTable and PriceLevelReuseTable for examples.
//
// A consequence of this is that IDs can't be used to uniquely map to an object. PriceLevels aren't mapped to directly
// by the price they represent and order IDs are reused.
//
// NodeList grouping:
//
// Ferum groups items in lists into single Nodes stored in a table to optimize read/write costs. See the NodeList
// structure for more details.
//
// Numeric values:
//
// Unless otherwise stated, all numeric values are fixedpoint number with 10 decimal places.
//
// Inlining:
//
// Certain modules are inlined but will potentially move to their own module files once the Move inline functionality
// is better finalized.
//
module ferum::market {
    use aptos_framework::coin;
    use aptos_framework::timestamp;
    use aptos_std::table;
    use std::signer::address_of;
    use std::vector;
    use std::string;
    use aptos_std::type_info;
    use aptos_framework::event::{EventHandle, emit_event};
    use aptos_framework::account::new_event_handle;
    use ferum::platform::AccountIdentifier;
    use ferum::platform;
    use ferum::token;
    use ferum::utils;

    #[test_only]
    use aptos_std::table_with_length as twl;
    #[test_only]
    use std::debug;
    #[test_only]
    use aptos_framework::account;
    #[test_only]
    use ferum::coin_test_helpers::{FMA, FMB, create_fake_coins, deposit_fake_coins};
    #[test_only]
    use ferum::test_utils as ftu;
    #[test_only]
    use ferum::test_utils::s;

    // <editor-fold defaultstate="collapsed" desc="Errors">

    // <editor-fold defaultstate="collapsed" desc="Market Errors">

    const ERR_COIN_UNINITIALIZED: u64 = 401;
    const ERR_UNKNOWN_ORDER: u64 = 402;
    const ERR_NOT_OWNER: u64 = 403;
    const ERR_COIN_EXCEEDS_MAX_SUPPORTED_DECIMALS: u64 = 404;
    const ERR_INVALID_BEHAVIOUR: u64 = 405;
    const ERR_INVALID_DECIMAL_CONFIG: u64 = 406;
    const ERR_INVALID_SIDE: u64 = 407;
    const ERR_ORDER_EXECUTED_BUT_IS_PENDING_CRANK: u64 = 408;
    const ERR_PRICE_STORE_ELEM_NOT_FOUND: u64 = 409;
    const ERR_CRANK_UNFULFILLED_QTY: u64 = 410;
    const ERR_NO_MARKET_ACCOUNT: u64 = 411;
    const ERR_INVALID_MAX_COLLATERAL_AMT: u64 = 412;

    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="Admin Errors">

    const ERR_NOT_ALLOWED: u64 = 200;
    const ERR_MARKET_NOT_EXISTS: u64 = 201;
    const ERR_MARKET_EXISTS: u64 = 202;
    const ERR_INVALID_FEE_TYPE: u64 = 203;
    const ERR_FEE_TYPE_EXISTS: u64 = 204;
    const ERR_FE_UNINITED: u64 = 205;

    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="Market Errors">

    const ERR_INVALID_FEE_STRUCTURE: u64 = 301;
    const ERR_TIER_NOT_FOUND: u64 = 302;

    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="Cache Errors">

    const ERR_CACHE_DUPLICATE_ITEM: u64 = 1;
    const ERR_CACHE_INVALID_TYPE: u64 = 2;
    const ERR_CACHE_ITEM_NOT_FOUND: u64 = 3;

    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="NodeList Errors">

    const ERR_LIST_EMPTY: u64 = 1;
    const ERR_LIST_ELEM_NOT_FOUND: u64 = 2;

    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="Tree Errors">

    const ERR_TREE_DUPLICATE_ITEM: u64 = 1;
    const ERR_TREE_ELEM_DOES_NOT_EXIST: u64 = 4;
    const ERR_TREE_INVALID_TREE_DEGREE: u64 = 5;
    const ERR_TREE_INVALID_ITERATOR_TYPE: u64 = 6;
    const ERR_TREE_EMPTY_ITERATOR: u64 = 7;
    const ERR_TREE_DUPLICATE_ENTRY: u64 = 8;

    // </editor-fold>

    // Needs to be included to prevent inline from complaining.
    // TODO: remove once inline is more finalized.
    const ERR_EXCEED_MAX_EXP: u64 = 1;
    const ERR_FP_PRECISION_LOSS: u64 = 2;
    const ERR_FP_EXCEED_DECIMALS: u64 = 3;

    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="Enums">

    // Order Side.
    // Represents a buy order.
    const SIDE_BUY: u8 = 1;
    // Represents a sell order.
    const SIDE_SELL: u8 = 2;

    // Order behaviour.
    // Represents a resting order. Can be a taker or a maker order.
    const BEHAVIOUR_GTC: u8 = 1;
    // Represents a POST order. Will always be the maker order.
    const BEHAVIOUR_POST: u8 = 2;
    // Represents a IOC order. Fills as much as possible and the rest is cancelled.
    // IOC orders are always takers.
    const BEHAVIOUR_IOC: u8 = 3;
    // Represents a FOK order. Either fills in its entirety or is cancelled.
    // FOK orders are always takers.
    const BEHAVIOUR_FOK: u8 = 4;

    // FixedPoint modes.
    // Ensures there is no precision loss past either specified number of decimals of the max decimal places.
    const FP_NO_PRECISION_LOSS: u8 = 1;
    // Rounds up if the number of decimals places exceeds the specified or max amount
    const FP_ROUND_UP: u8 = 2;
    const FP_TRUNC: u8 = 3;

    // B tree child types used for deletions.
    const CHILD_TYPE_NULL: u8 = 0;
    const CHILD_TYPE_LEFT: u8 = 1;
    const CHILD_TYPE_RIGHT: u8 = 2;

    // B tree iterator types.
    const DECREASING_ITERATOR: u8 = 1; // Used for buys.
    const INCREASING_ITERATOR: u8 = 2; // Used for sells.

    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="Constants">

    const MAX_DECIMALS: u8 = 10;
    const TREE_DEGREE: u64 = 8;
    const PRICE_LEVEL_NODE_SIZE: u8 = 20;
    const EVENT_QUEUE_NODE_SIZE: u8 = 5;
    const DECIMAL_PLACES: u8 = 10;
    const DECIMAL_PLACES_EXP_U128: u128 = 10000000000;
    const DECIMAL_PLACES_EXP_U64: u64 = 10000000000;
    const MAX_U64: u64 = 18446744073709551615;
    const MAX_U16: u64 = 65535;

    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="Market implementation">

    // Structs.

    // Each market account is uniquely described by a protocol and user address.
    struct MarketAccountKey has store, copy, drop {
        protocolAddress: address,
        userAddress: address,
    }

    // The market account itself.
    struct MarketAccount<phantom I, phantom Q> has store {
        // List of ids of orders which are still active for this account.
        activeOrders: vector<u32>,
        // The total instrument coin balance for this order.
        instrumentBalance: coin::Coin<I>,
        // The total quote coin balance for this order.
        quoteBalance: coin::Coin<Q>,
        // Signer that created this market account.
        ownerAddress: address,
    }

    // Stores all the properties defining an order.
    struct OrderMetadata has drop, copy, store {
        // Side for this order. See the OrderSide enum.
        side: u8,
        // Behaviour for this order. See the OrderBehaviour enum.
        behaviour: u8,
        // Limit price for this order.
        price: u64,
        // The original quantity of the order.
        originalQty: u64,
        // The remaining qty of the order + any pending crank qty. The order is fully filled if
        // unfilledQty - <pending crank qty> == 0. <pending crank qty> is equal to the takerCrankPendingQty, stored on
        // the order, and the maker crank pending qty, which is only stored in the price store for efficiancy.
        unfilledQty: u64,
        // Qty of a taker order which was executed but is still waiting for the crank to be turned so executions can
        // be created.
        takerCrankPendingQty: u64,
        // Optional metadata provided for this order.
        clientOrderID: u32,
        // Address of the owner of this order. Could be the user's address directly or the address of a protocol
        // owned resource account.
        ownerAddress: address,
        // Key of market account this order was placed for.
        accountKey: MarketAccountKey,
        // The remaining collateral for a market buy order.
        marketBuyRemainingCollateral: u64,
    }

    // Structural information about the order in addition to its metadata.
    struct Order<phantom I, phantom Q> has store {
        // Metadata of the order.
        metadata: OrderMetadata,
        // The buy collateral for this order. Will be 0 if the order is a sell order.
        buyCollateral: coin::Coin<Q>,
        // The sell collateral for this order. Will be 0 if the order is a buy order.
        sellCollateral: coin::Coin<I>,
        // The id of the price level this order belongs to. Note that the id can be 0 even when the order is in use.
        // This happens when the order is a taker order with filled qty which is still pending a crank turn but was
        // never added to the book.
        priceLevelID: u16,
        // Pointer to the next order in the unused stack. 0 if this order is currently in use.
        next: u32,
    }

    // The representation of the order in a price level object.
    struct PriceLevelOrder has store, copy, drop {
        // The id of the order.
        id: u32,
        // The unfilled qty of the order. Note that this qty also includes qty that has been executed but is still
        // pending a crank turn.
        qty: u64,
    }

    // Object representing a single price level.
    struct PriceLevel has store {
        // The list of orders at this price level.
        orders: NodeList<PriceLevelOrder>,
        // Pointer to the next price level in the unused stack. 0 if this
        // price level is currently in use.
        next: u16,
    }

    // Element stored in the price stores (cache and tree).
    struct PriceStoreElem has store, copy, drop {
        // The total qty available at this price.
        qty: u64,
        // The qty that has already been allocated to executions but are still waiting on the crank to turn.
        makerCrankPendingQty: u64,
        // The node id of the PriceLevel corresponding to this level. The PriceLevel object stores a list of
        // orders and their quantities.
        priceLevelID: u16,
    }

    // Reuse table storing price levels.
    struct PriceLevelReuseTable has store {
        objects: table::Table<u16, PriceLevel>,
        unusedStack: u16,
        currID: u16,
    }

    // Reuse table storing orders.
    struct OrderReuseTable<phantom I, phantom Q> has store {
        objects: table::Table<u32, Order<I, Q>>,
        unusedStack: u32,
        currID: u32,
    }

    // Summary information about the market. Primarily used to aboid having to load both the tree and the cache when
    // processing new orders and order cancels.
    struct MarketSummary has store {
        // Total unfilled qty in the sell cache. Does not include qty pending a crank turn.
        sellCacheQty: u64,
        // Total unfilled qty in the buy cache. Does not include qty pending a crank turn.
        buyCacheQty: u64,
        // The minimum price in the sell tree. Includes price levels still in the tree due to some pending crank qty.
        sellTreeMin: u64,
        // The maximum price in the buy tree. Includes price levels still in the tree due to somepending crank qty.
        buyTreeMax: u64,
        // The maximum price in the sell cache. Includes price levels still in the tree due to some pending crank qty.
        sellCacheMax: u64,
        // The minimum price in the sell cache. Includes price levels still in the tree due to some pending crank qty.
        sellCacheMin: u64,
        // The maximum price in the buy cache. Includes price levels still in the tree due to some pending crank qty.
        buyCacheMax: u64,
        // The minimum price in the buy cache. Includes price levels still in the tree due to some pending crank qty.
        buyCacheMin: u64,
        // The number of prices in the buy cache. Includes price levels with pending crank qty.
        buyCacheSize: u8,
        // The number of prices in the sell cache. Includes price levels with pending crank qty.
        sellCacheSize: u8,
    }

    // Struct encapsilating all info for a market.
    struct Orderbook<phantom I, phantom Q> has key, store {
        // Top level summary variables for this market. Used primarily to avoid having to load both the table and
        // cache.
        summary: MarketSummary,
        // The maximum number of elements that can go in the cache.
        maxCacheSize: u8,
        // Number of decimals for the instrument coin.
        iDecimals: u8,
        // Number of decimals for the quote coin.
        qDecimals: u8,
        // Fee type for this market.
        feeType: string::String,
        // All the price levels for this book.
        priceLevelsTable: PriceLevelReuseTable,
        // All the orders for this book.
        ordersTable: OrderReuseTable<I, Q>,
        // Market accounts for this trading pair.
        marketAccounts: table::Table<MarketAccountKey, MarketAccount<I, Q>>,
    }

    // Queue of execution events.
    struct ExecutionQueueEvent has store, drop, copy {
        qty: u64,
        takerOrderID: u32,
        priceLevelID: u16,
        timestampSecs: u64,
    }

    // Queue of events which are consumed by the crank.
    struct EventQueue<phantom I, phantom Q> has key {
        queue: NodeList<ExecutionQueueEvent>,
    }

    // Representation of an execution emitted as an Aptos event.
    struct IndexingExecutionEvent has store, drop {
        makerAccountKey: MarketAccountKey,
        takerAccountKey: MarketAccountKey,
        price: u64, // Fixedpoint value.
        qty: u64, // Fixedpoint value.
        timestampSecs: u64
    }

    // Representation of an order finalization emitted as an Aptos event.
    struct IndexingFinalizeEvent has store, drop {
        accountKey: MarketAccountKey,
        originalQty: u64,
        price: u64, // Fixedpoint value.
        timestampSecs: u64,
    }

    // Wrapper to store all event handles.
    struct IndexingEventHandles<phantom I, phantom Q> has key {
        executions: EventHandle<IndexingExecutionEvent>,
        finalizations: EventHandle<IndexingFinalizeEvent>
    }

    // Struct encapsulating price at a given timestamp for the market.
    // TODO: add indexing method.
    struct PriceUpdateEvent has drop, store {
        // Type info for the instrument coin type for the order.
        instrumentType: type_info::TypeInfo,
        // Type info for the quote coin type for the order.
        quoteType: type_info::TypeInfo,
        // The most someone is willing to pay for the given instrument/quote pair.
        // Represented as a fixed point number.
        maxBid: u64,
        // How much quantity there is the the maxBid price point.
        // Represented as a fixed point number.
        bidSize: u64,
        // The least someone is willing to accept as payment for the given instrument/quote pair.
        // Represented as a fixed point number.
        minAsk: u64,
        // How much quantity there is the the minAsk price point.
        // Represented as a fixed point number.
        askSize: u64,
        // The chain timestamp this quote was issued at.
        timestampMicroSeconds: u64
    }

    // Wrapper objects around the tree/cache. Stored as seperate items in global storage to avoid having to load each
    // one individually.
    struct MarketBuyTree<phantom I, phantom Q> has key, store {
        tree: Tree<PriceStoreElem>,
    }
    struct MarketSellTree<phantom I, phantom Q> has key, store {
        tree: Tree<PriceStoreElem>,
    }
    struct MarketBuyCache<phantom I, phantom Q> has key, store {
        cache: Cache<PriceStoreElem>,
    }
    struct MarketSellCache<phantom I, phantom Q> has key, store {
        cache: Cache<PriceStoreElem>,
    }

    // <editor-fold defaultstate="collapsed" desc="Market entry functions">

    public entry fun init_market_entry<I, Q>(
        owner: &signer,
        instrumentDecimals: u8,
        quoteDecimals: u8,
        maxCacheSize: u8,
        feeType: string::String
    ) acquires FerumInfo {
        let ownerAddr = address_of(owner);
        assert!(ownerAddr == @ferum, ERR_NOT_ALLOWED);
        let (iCoinDecimals, qCoinDecimals) = validate_coins<I, Q>();
        assert!(iCoinDecimals >= instrumentDecimals && qCoinDecimals >= quoteDecimals, ERR_INVALID_DECIMAL_CONFIG);
        let minDecimals = if (iCoinDecimals < qCoinDecimals) {
            iCoinDecimals
        } else {
            qCoinDecimals
        };
        assert!(instrumentDecimals + quoteDecimals <= minDecimals, ERR_INVALID_DECIMAL_CONFIG);
        get_fee_structure(feeType); // Asserts that the feeType maps to a valid fee structure.

        let finalizeEvents = new_event_handle<IndexingFinalizeEvent>(owner);
        let executionEvents = new_event_handle<IndexingExecutionEvent>(owner);
        move_to(owner, Orderbook<I, Q>{
            summary: MarketSummary {
                sellCacheQty: 0,
                buyCacheQty: 0,
                sellCacheMax: 0,
                buyCacheMax: 0,
                buyCacheMin: 0,
                sellCacheMin: 0,
                buyTreeMax: 0,
                sellTreeMin: 0,
                buyCacheSize: 0,
                sellCacheSize: 0,
            },
            maxCacheSize,
            iDecimals: instrumentDecimals,
            qDecimals: quoteDecimals,
            feeType,
            priceLevelsTable: PriceLevelReuseTable {
                objects: table::new(),
                unusedStack: 0,
                currID: 1,
            },
            ordersTable: OrderReuseTable<I, Q>{
                objects: table::new(),
                unusedStack: 0,
                currID: 1,
            },
            marketAccounts: table::new(),
        });
        move_to(owner, MarketBuyTree<I, Q>{
            tree: new_tree(TREE_DEGREE),
        });
        move_to(owner, MarketSellTree<I, Q>{
            tree: new_tree(TREE_DEGREE),
        });
        move_to(owner, MarketBuyCache<I, Q>{
            cache: new_cache(SIDE_BUY),
        });
        move_to(owner, MarketSellCache<I, Q>{
            cache: new_cache(SIDE_SELL),
        });
        move_to(owner, EventQueue<I, Q>{
            queue: new_list(EVENT_QUEUE_NODE_SIZE),
        });
        move_to(owner, IndexingEventHandles<I, Q>{
            executions: executionEvents,
            finalizations: finalizeEvents,
        });
        register_market<I, Q>(ownerAddr);
    }

    public entry fun open_market_account_entry<I, Q>(
        owner: &signer
    ) acquires FerumInfo, Orderbook {
        open_market_account<I, Q>(owner, vector[]);
    }

    public entry fun deposit_to_market_account_entry<I, Q>(
        owner: &signer,
        coinIAmt: u64,
        coinQAmt: u64,
    ) acquires FerumInfo, Orderbook {
        let accountKey = MarketAccountKey {
            protocolAddress: @0,
            userAddress: address_of(owner),
        };
        deposit_to_market_account<I, Q>(owner, accountKey, coinIAmt, coinQAmt)
    }

    public entry fun rebalance_cache_entry<I, Q>(
        _: &signer,
        limit: u8,
    ) acquires Orderbook, MarketSellCache, MarketSellTree, MarketBuyCache, MarketBuyTree, FerumInfo {
        let marketAddr = get_market_addr<I, Q>();
        let book = borrow_global_mut<Orderbook<I, Q>>(marketAddr);

        if (book.summary.buyCacheSize < book.maxCacheSize) {
            rebalance_cache(
                book,
                SIDE_BUY,
                &mut borrow_global_mut<MarketBuyCache<I, Q>>(marketAddr).cache,
                &mut borrow_global_mut<MarketBuyTree<I, Q>>(marketAddr).tree,
                limit,
            );
        };
        if (book.summary.sellCacheSize < book.maxCacheSize) {
            rebalance_cache(
                book,
                SIDE_SELL,
                &mut borrow_global_mut<MarketSellCache<I, Q>>(marketAddr).cache,
                &mut borrow_global_mut<MarketSellTree<I, Q>>(marketAddr).tree,
                limit,
            );
        };
    }

    public entry fun withdraw_from_market_account_entry<I, Q>(
        owner: &signer,
        coinIAmt: u64, // Fixedpoint value.
        coinQAmt: u64, // Fixedpoint value.
    ) acquires FerumInfo, Orderbook {
        let accountKey = MarketAccountKey {
            protocolAddress: @0,
            userAddress: address_of(owner),
        };
        withdraw_from_market_account<I, Q>(owner, accountKey, coinIAmt, coinQAmt)
    }

    public entry fun crank<I, Q>(
        _: &signer,
        limit: u8,
    ) acquires FerumInfo, Orderbook, EventQueue, IndexingEventHandles, MarketBuyTree, MarketBuyCache, MarketSellTree, MarketSellCache {
        let marketAddr = get_market_addr<I, Q>();
        let i = 0;
        let queue = &mut borrow_global_mut<EventQueue<I, Q>>(marketAddr).queue;
        let book = borrow_global_mut<Orderbook<I, Q>>(marketAddr);
        let sellCache = &mut borrow_global_mut<MarketSellCache<I, Q>>(marketAddr).cache;
        let buyCache = &mut borrow_global_mut<MarketBuyCache<I, Q>>(marketAddr).cache;
        let sellTree = &mut borrow_global_mut<MarketSellTree<I, Q>>(marketAddr).tree;
        let buyTree = &mut borrow_global_mut<MarketBuyTree<I, Q>>(marketAddr).tree;
        let eventHandles = borrow_global_mut<IndexingEventHandles<I, Q>>(marketAddr);
        let feeStructure = get_fee_structure(book.feeType);
        let instrumentDecimals = coin::decimals<I>();
        let quoteDecimals = coin::decimals<Q>();
        while (i < limit) {
            let nodeElemsReversed = list_pop_node_reversed(queue);
            let j = 0;
            let size = vector::length(&nodeElemsReversed);
            while (j < size) {
                let event = vector::pop_back(&mut nodeElemsReversed);
                processExecEvent(
                    book,
                    buyCache,
                    sellCache,
                    buyTree,
                    sellTree,
                    &event,
                    &mut eventHandles.executions,
                    &mut eventHandles.finalizations,
                    instrumentDecimals,
                    quoteDecimals,
                    feeStructure,
                );
                j = j + 1;
                i = i + 1;
            };
        };
        // Note that crank turning will not remove or add qty to the cache.
        update_cache_size_and_qty(&mut book.summary, buyCache, 0, 0);
        update_cache_size_and_qty(&mut book.summary, sellCache, 0, 0);
        update_cache_max_min(&mut book.summary, buyCache);
        update_cache_max_min(&mut book.summary, sellCache);
        update_tree_max_min(&mut book.summary, buyTree, SIDE_BUY);
        update_tree_max_min(&mut book.summary, sellTree, SIDE_SELL);
    }

    public entry fun add_order_entry<I, Q>(
        owner: &signer,
        side: u8,
        behaviour: u8,
        price: u64, // Fixedpoint value.
        qty: u64, // Fixedpoint value.
        clientOrderID: u32,
        marketBuyMaxCollateral: u64, // Should only be specified for market orders.
    ) acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles {
        let accountKey = MarketAccountKey {
            protocolAddress: @ferum,
            userAddress: address_of(owner),
        };
        add_order<I, Q>(owner, accountKey, side, behaviour, price, qty, clientOrderID, marketBuyMaxCollateral);
    }

    public entry fun cancel_order_entry<I, Q>(
        owner: &signer,
        orderID: u32,
    ) acquires FerumInfo, Orderbook, MarketBuyTree, MarketBuyCache, MarketSellTree, MarketSellCache, IndexingEventHandles {
        cancel_order<I, Q>(owner, orderID);
    }

    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="Market public functions">

    public fun open_market_account<I, Q>(
        owner: &signer,
        id: vector<platform::AccountIdentifier>
    ): MarketAccountKey acquires FerumInfo, Orderbook {
        let ownerAddr = address_of(owner);
        if (!coin::is_account_registered<token::Fe>(ownerAddr)) {
            coin::register<token::Fe>(owner);
        };
        let marketAddr = get_market_addr<I, Q>();
        let book = borrow_global_mut<Orderbook<I, Q>>(marketAddr);
        let accountKey = if (vector::length(&id) > 0) {
            account_key_from_identifier(vector::pop_back(&mut id))
        } else {
            MarketAccountKey {
                protocolAddress: @ferum,
                userAddress: ownerAddr,
            }
        };
        table::add(&mut book.marketAccounts, accountKey, MarketAccount<I, Q>{
            activeOrders: vector[],
            instrumentBalance: coin::zero(),
            quoteBalance: coin::zero(),
            ownerAddress: ownerAddr,
        });
        accountKey
    }

    public fun deposit_to_market_account<I, Q>(
        owner: &signer,
        accountKey: MarketAccountKey,
        coinIAmt: u64, // Fixedpoint value.
        coinQAmt: u64, // Fixedpoint value.
    ) acquires FerumInfo, Orderbook {
        let marketAddr = get_market_addr<I, Q>();
        let book = borrow_global_mut<Orderbook<I, Q>>(marketAddr);
        assert!(table::contains(&book.marketAccounts, accountKey), ERR_NO_MARKET_ACCOUNT);
        let marketAcc = table::borrow_mut(&mut book.marketAccounts, accountKey);
        assert!(owns_account(owner, &accountKey, marketAcc), ERR_NOT_OWNER);
        if (coinIAmt > 0) {
            let coinIDecimals = coin::decimals<I>();
            let coinAmt = coin::withdraw<I>(owner, utils::fp_convert(coinIAmt, coinIDecimals, FP_NO_PRECISION_LOSS));
            coin::merge(&mut marketAcc.instrumentBalance, coinAmt);
        };
        if (coinQAmt > 0) {
            let coinQDecimals = coin::decimals<Q>();
            let coinAmt = coin::withdraw<Q>(owner, utils::fp_convert(coinQAmt, coinQDecimals, FP_NO_PRECISION_LOSS));
            coin::merge(&mut marketAcc.quoteBalance, coinAmt);
        };
    }

    public fun withdraw_from_market_account<I, Q>(
        owner: &signer,
        accountKey: MarketAccountKey,
        coinIAmt: u64, // Fixedpoint value.
        coinQAmt: u64, // Fixedpoint value.
    ) acquires FerumInfo, Orderbook {
        let marketAddr = get_market_addr<I, Q>();
        let book = borrow_global_mut<Orderbook<I, Q>>(marketAddr);
        assert!(table::contains(&book.marketAccounts, accountKey), ERR_NO_MARKET_ACCOUNT);
        let marketAcc = table::borrow_mut(&mut book.marketAccounts, accountKey);
        assert!(owns_account(owner, &accountKey, marketAcc), ERR_NOT_OWNER);
        if (coinIAmt > 0) {
            let coinIDecimals = coin::decimals<I>();
            let coinAmt = coin::withdraw<I>(owner, utils::fp_convert(coinIAmt, coinIDecimals, FP_NO_PRECISION_LOSS));
            coin::merge(&mut marketAcc.instrumentBalance, coinAmt);
        };
        if (coinQAmt > 0) {
            let coinQDecimals = coin::decimals<Q>();
            let coinAmt = coin::withdraw<Q>(owner, utils::fp_convert(coinQAmt, coinQDecimals, FP_NO_PRECISION_LOSS));
            coin::merge(&mut marketAcc.quoteBalance, coinAmt);
        };
    }

    public fun add_order<I, Q>(
        owner: &signer,
        accountKey: MarketAccountKey,
        side: u8,
        behaviour: u8,
        price: u64, // Fixedpoint value.
        qty: u64, // Fixedpoint value.
        clientOrderID: u32,
        marketBuyMaxCollateral: u64, // Should only be specified for market orders.
    ): u32 acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles {
        let marketAddr = get_market_addr<I, Q>();
        let book = borrow_global_mut<Orderbook<I, Q>>(marketAddr);
        let execs = &mut vector[];
        let orderID = add_order_to_book<I, Q>(
            owner,
            accountKey,
            marketAddr,
            behaviour,
            side,
            price,
            qty,
            clientOrderID,
            marketBuyMaxCollateral,
            book,
            execs,
        );
        // Add any created execution events to queue.
        let execCount = vector::length(execs);
        if (execCount > 0) {
            let queue = &mut borrow_global_mut<EventQueue<I, Q>>(marketAddr).queue;
            let i = 0;
            let halfExecCount = execCount / 2;
            while (i < halfExecCount) { // Inlined reverse.
                vector::swap(execs, i, execCount - i - 1);
                i = i + 1;
            };
            i = 0;
            while (i < execCount) {
                let exec = vector::pop_back(execs);
                list_push(queue, exec);
                i = i + 1;
            };
        };
        orderID
    }

    public inline fun prealloc_price_levels(
        table: &mut PriceLevelReuseTable,
        count: u8,
    ) {
        let i = 0;
        while (i < count) {
            let priceLevel = PriceLevel {
                orders: new_list(PRICE_LEVEL_NODE_SIZE),
                next: table.unusedStack,
            };
            let priceLevelID = table.currID;
            table.currID = table.currID + 1;
            table.unusedStack = priceLevelID;
            table::add(&mut table.objects, priceLevelID, priceLevel);
            i = i + 1;
        }
    }

    public inline fun prealloc_orders<I, Q>(
        table: &mut OrderReuseTable<I, Q>,
        count: u8,
    ) {
        let i = 0;
        while (i < count) {
            let order = Order<I, Q>{
                next: table.unusedStack,
                buyCollateral: coin::zero(),
                sellCollateral: coin::zero(),
                priceLevelID: 0,
                metadata: OrderMetadata{
                    side: 0,
                    behaviour: 0,
                    price: 0,
                    originalQty: 0,
                    unfilledQty: 0,
                    takerCrankPendingQty: 0,
                    clientOrderID: 0,
                    accountKey: sentinal_market_account_key(),
                    ownerAddress: @0,
                    marketBuyRemainingCollateral: 0,
                },
            };
            let orderID = table.currID;
            table.currID = table.currID + 1;
            table.unusedStack = orderID;
            table::add(&mut table.objects, orderID, order);
            i = i + 1;
        }
    }

    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="Market private functions">

    fun processExecEvent<I, Q>(
        book: &mut Orderbook<I, Q>,
        buyCache: &mut Cache<PriceStoreElem>,
        sellCache: &mut Cache<PriceStoreElem>,
        buyTree: &mut Tree<PriceStoreElem>,
        sellTree: &mut Tree<PriceStoreElem>,
        event: &ExecutionQueueEvent,
        execEventHandle: &mut EventHandle<IndexingExecutionEvent>,
        finalizeEventHandle: &mut EventHandle<IndexingFinalizeEvent>,
        instrumentDecimals: u8,
        quoteDecimals: u8,
        feeStructure: &FeeStructure,
    ) {
        let priceLevelsTable = &mut book.priceLevelsTable;
        let ordersTable = &mut book.ordersTable;
        let marketAccounts = &mut book.marketAccounts;
        let makerPriceLevel = table::borrow_mut(&mut priceLevelsTable.objects, event.priceLevelID);
        let currNodeID = makerPriceLevel.orders.head;
        // Get taker user address.
        let takerOrder = table::borrow_mut(&mut ordersTable.objects, event.takerOrderID);
        let takerPrice = takerOrder.metadata.price;
        let takerBuyCollateral = coin::extract_all(&mut takerOrder.buyCollateral);
        let takerSellCollateral = coin::extract_all(&mut takerOrder.sellCollateral);
        let takerInstrumentProceeds = coin::zero<I>();
        let takerQuoteProceeds = coin::zero<Q>();
        let takerAccountKey = takerOrder.metadata.accountKey;
        let takerUserAddress = takerAccountKey.userAddress;
        let takerProtocolAddress = takerAccountKey.protocolAddress;
        let takerUserFeBalance = if (!coin::is_account_registered<token::Fe>(takerUserAddress)) {
            0
        } else {
            coin::balance<token::Fe>(takerUserAddress)
        };
        let (_, _takerUserFee) = get_user_fee(feeStructure, takerUserFeBalance);
        let takerProtocolFeBalance = if (!coin::is_account_registered<token::Fe>(takerProtocolAddress)) {
            0
        } else {
            coin::balance<token::Fe>(takerProtocolAddress)
        };
        let _takerProtocolSplit = get_protocol_fee(feeStructure, takerProtocolFeBalance);
        // First, update the price level and orders in the price level.
        let qty = event.qty;
        let numElemsToDrop = 0;
        while (qty > 0) {
            let currNode = table::borrow_mut(&mut makerPriceLevel.orders.nodes, currNodeID);
            let i = 0;
            let size = vector::length(&currNode.data);
            while (i < size && qty > 0) {
                let orderElem = vector::borrow_mut(&mut currNode.data, i);
                let execFillQty = if (qty > orderElem.qty) {
                    orderElem.qty
                } else {
                    qty
                };
                qty = qty - execFillQty;
                orderElem.qty = orderElem.qty - execFillQty;
                // Update the maker order.
                let makerOrderID = orderElem.id;
                let makerOrder = table::borrow_mut(&mut ordersTable.objects, makerOrderID);
                let makerPrice = makerOrder.metadata.price;
                let makerSide = makerOrder.metadata.side;
                makerOrder.metadata.unfilledQty = makerOrder.metadata.unfilledQty - execFillQty;
                let makerAccountKey = makerOrder.metadata.accountKey;
                let makerAccount = table::borrow_mut(marketAccounts, makerAccountKey);
                let makerUserAddress = makerAccountKey.userAddress;
                let makerProtocolAddress = makerAccountKey.protocolAddress;
                let makerUserFeBalance = if (!coin::is_account_registered<token::Fe>(makerUserAddress)) {
                    0
                } else {
                    coin::balance<token::Fe>(makerUserAddress)
                };
                let (_, _makerUserFee) = get_user_fee(feeStructure, makerUserFeBalance);
                let makerProtocolFeBalance = if (!coin::is_account_registered<token::Fe>(makerProtocolAddress)) {
                    0
                } else {
                    coin::balance<token::Fe>(makerProtocolAddress)
                };
                let _makerProtocolSplit = get_protocol_fee(feeStructure, makerProtocolFeBalance);
                // Settle execution and update the price store element.
                if (makerSide == SIDE_BUY) {
                    // Settle.
                    // Give the maker instrument coin.
                    let instrAmt = utils::fp_convert(execFillQty, instrumentDecimals, FP_NO_PRECISION_LOSS);
                    let instrCoinAmt = coin::extract(&mut takerSellCollateral, instrAmt);
                    coin::merge(&mut makerAccount.instrumentBalance, instrCoinAmt);
                    // TODO: charge maker fee.
                    // Give the taker quote coin.
                    let quoteAmt = utils::fp_convert(utils::fp_mul(makerPrice, execFillQty, FP_NO_PRECISION_LOSS), quoteDecimals, FP_NO_PRECISION_LOSS);
                    let quoteCoinAmt = coin::extract(&mut makerOrder.buyCollateral, quoteAmt);
                    coin::merge(&mut takerQuoteProceeds, quoteCoinAmt);
                    // TODO: charge taker fee.
                } else {
                    // Settle.
                    // Give the maker quote coin.
                    let quoteAmt = utils::fp_convert(utils::fp_mul(makerPrice, execFillQty, FP_NO_PRECISION_LOSS), quoteDecimals, FP_NO_PRECISION_LOSS);
                    let quoteCoinAmt = coin::extract(&mut takerBuyCollateral, quoteAmt);
                    coin::merge(&mut makerAccount.quoteBalance, quoteCoinAmt);
                    // TODO: charge maker fee.
                    // Give the taker instrument coin.
                    let instrAmt = utils::fp_convert(execFillQty, instrumentDecimals, FP_NO_PRECISION_LOSS);
                    let instrCoinAmt = coin::extract(&mut makerOrder.sellCollateral, instrAmt);
                    coin::merge(&mut takerInstrumentProceeds, instrCoinAmt);
                    if (takerPrice != 0 && takerPrice > makerPrice) {
                        // Pre-emptively release collateral to the taker because it can't be used (due to limit price).
                        let takerMakerPriceDiff = takerPrice - makerPrice;
                        let excessCollateral = utils::fp_convert(utils::fp_mul(takerMakerPriceDiff, execFillQty, FP_NO_PRECISION_LOSS), quoteDecimals, FP_NO_PRECISION_LOSS);
                        if (excessCollateral > 0) {
                            let excessCollateralAmt = coin::extract(&mut takerBuyCollateral, excessCollateral);
                            coin::merge(&mut takerQuoteProceeds, excessCollateralAmt);
                        };
                    };
                    // TODO: charge taker fee.
                };
                // Update price store and price level.
                if (is_price_store_elem_in_cache(&book.summary, makerSide, makerPrice)) {
                    // The price for the order is in the cache.
                    let cache = if (makerSide == SIDE_BUY) {
                        buyCache
                    } else {
                        sellCache
                    };
                    let res = cache_find(cache, makerPrice);
                    assert!(vector::length(&res) > 0, ERR_PRICE_STORE_ELEM_NOT_FOUND);
                    remove_price_qty_from_cache(
                        cache,
                        makerPrice,
                        execFillQty,
                        true,
                        res,
                    );
                    // We update summary variables in the cancel method.
                } else {
                    // The price for the order is in the tree.
                    let tree = if (makerSide == SIDE_BUY) {
                        buyTree
                    } else {
                        sellTree
                    };
                    let pos = tree_find(tree, makerPrice);
                    assert!(pos.nodeID != 0, ERR_PRICE_STORE_ELEM_NOT_FOUND);
                    remove_price_qty_from_tree(
                        tree,
                        makerPrice,
                        execFillQty,
                        true,
                        vector[pos],
                    );
                    // We update summary variables in the cancel method.
                };
                // Emit an execution event.
                emit_event(execEventHandle, IndexingExecutionEvent {
                    makerAccountKey,
                    takerAccountKey,
                    price: makerOrder.metadata.price,
                    qty: execFillQty,
                    timestampSecs: event.timestampSecs,
                });
                // If the maker order is finalized and all crank pending qty has been flushed, add the order to the
                // unused order stack, and emit a finalize event.
                // Shouldn't have to worry about market orders because they will never be in the book.
                if (is_finalized(makerOrder)) {
                    emit_event(finalizeEventHandle, IndexingFinalizeEvent {
                        accountKey: makerAccountKey,
                        price: makerOrder.metadata.price,
                        originalQty: makerOrder.metadata.originalQty,
                        timestampSecs: event.timestampSecs,
                    });
                    makerOrder.metadata = OrderMetadata {
                        side: 0,
                        behaviour: 0,
                        price: 0,
                        originalQty: 0,
                        unfilledQty: 0,
                        takerCrankPendingQty: 0,
                        clientOrderID: 0,
                        accountKey: sentinal_market_account_key(),
                        ownerAddress: @0,
                        marketBuyRemainingCollateral: 0,
                    };
                    // Release any unsettled collateral.
                    if (coin::value(&makerOrder.buyCollateral) > 0) {
                        coin::merge(&mut makerAccount.quoteBalance, coin::extract_all(&mut makerOrder.buyCollateral));
                    };
                    if (coin::value(&makerOrder.sellCollateral) > 0) {
                        coin::merge(&mut makerAccount.instrumentBalance, coin::extract_all(&mut makerOrder.sellCollateral));
                    };
                    makerOrder.priceLevelID = 0;
                    makerOrder.next = ordersTable.unusedStack;
                    ordersTable.unusedStack = makerOrderID;
                };
                numElemsToDrop = numElemsToDrop + 1;
                i = i + 1;
            };
        };
        assert!(qty == 0, ERR_CRANK_UNFULFILLED_QTY);
        // Update the taker order.
        let takerAccount = table::borrow_mut(marketAccounts, takerAccountKey);
        let takerOrder = table::borrow_mut(&mut ordersTable.objects, event.takerOrderID);
        takerOrder.metadata.takerCrankPendingQty = takerOrder.metadata.takerCrankPendingQty - event.qty;
        takerOrder.metadata.unfilledQty = takerOrder.metadata.unfilledQty - event.qty;
        // Tranfer over proceeds from the taker order.
        coin::merge(&mut takerAccount.instrumentBalance, takerInstrumentProceeds);
        coin::merge(&mut takerAccount.quoteBalance, takerQuoteProceeds);
        // Merge back the borrowed collateral.
        coin::merge(&mut takerOrder.buyCollateral, takerBuyCollateral);
        coin::merge(&mut takerOrder.sellCollateral, takerSellCollateral);
        // If the taker order is finalized and all crank pending qty has been flushed, add the order to the
        // unused order stack, and emit a finalize event.
        if (is_finalized(takerOrder)) {
            emit_event(finalizeEventHandle, IndexingFinalizeEvent {
                accountKey: takerAccountKey,
                price: takerOrder.metadata.price,
                originalQty: takerOrder.metadata.originalQty,
                timestampSecs: event.timestampSecs,
            });
            takerOrder.metadata = OrderMetadata {
                side: 0,
                behaviour: 0,
                price: 0,
                originalQty: 0,
                unfilledQty: 0,
                takerCrankPendingQty: 0,
                clientOrderID: 0,
                accountKey: sentinal_market_account_key(),
                ownerAddress: @0,
                marketBuyRemainingCollateral: 0,
            };
            // Release any unsettled collateral.
            if (coin::value(&takerOrder.buyCollateral) > 0) {
                coin::merge(&mut takerAccount.quoteBalance, coin::extract_all(&mut takerOrder.buyCollateral));
            };
            if (coin::value(&takerOrder.sellCollateral) > 0) {
                coin::merge(&mut takerAccount.instrumentBalance, coin::extract_all(&mut takerOrder.sellCollateral));
            };
            takerOrder.priceLevelID = 0;
            takerOrder.next = ordersTable.unusedStack;
            ordersTable.unusedStack = event.takerOrderID;
        };
        // Remove orders from the PriceLevel.
        list_drop_from_front(&mut makerPriceLevel.orders, numElemsToDrop);
    }

    fun rebalance_cache<I, Q>(
        book: &mut Orderbook<I, Q>,
        side: u8,
        cache: &mut Cache<PriceStoreElem>,
        tree: &mut Tree<PriceStoreElem>,
        limit: u8,
    ) {
        let i = 0;
        while (i < limit) {
            let cacheSize = if (side == SIDE_BUY) {
                book.summary.buyCacheSize
            } else {
                book.summary.sellCacheSize
            };
            if (cacheSize >= book.maxCacheSize) {
                break
            };
            if (tree.treeSize == 0) {
                break
            };
            // Pop from tree.
            let (price, elem) = if (side == SIDE_BUY) {
                tree_pop_max(tree)
            } else {
                tree_pop_min(tree)
            };
            let qty = elem.qty;
            update_tree_max_min(&mut book.summary, tree, side);
            // Push into cache.
            cache_insert(cache, price, elem);
            update_cache_max_min(&mut book.summary, cache);
            update_cache_size_and_qty(&mut book.summary, cache, 0, qty);

            i = i + 1;
        }
    }

    fun update_cache_size_and_qty(
        summary: &mut MarketSummary,
        cache: &Cache<PriceStoreElem>,
        qtyRemoved: u64,
        qtyAdded: u64,
    ) {
        let size = vector::length(&cache.list);
        if (cache.side == SIDE_BUY) {
            summary.buyCacheSize = (size as u8);
            summary.buyCacheQty = summary.buyCacheQty + qtyAdded - qtyRemoved;
        } else {
            summary.sellCacheSize = (size as u8);
            summary.sellCacheQty = summary.sellCacheQty + qtyAdded - qtyRemoved;
        };
    }

    fun update_cache_max_min(
        summary: &mut MarketSummary,
        cache: &Cache<PriceStoreElem>,
    ) {
        // Reset buyCacheMax/sellCacheMin and buyCacheMin/sellCacheMax.
        if (cache.side == SIDE_BUY) {
            summary.buyCacheMax = 0;
            summary.buyCacheMin = 0;
        } else {
            summary.sellCacheMin = 0;
            summary.sellCacheMax = 0;
        };
        let size = vector::length(&cache.list);
        if (size == 0) {
            return
        };
        // First update buyCacheMax/sellCacheMin.
        let elem = vector::borrow(&cache.list, size-1);
        if (cache.side == SIDE_BUY) {
            summary.buyCacheMax = elem.key;
        } else {
            summary.sellCacheMin = elem.key;
        };
        // Then, update buyCacheMin/sellCacheMax.
        let elem = vector::borrow(&cache.list, 0);
        if (cache.side == SIDE_BUY) {
            summary.buyCacheMin = elem.key;
        } else {
            summary.sellCacheMax = elem.key;
        };
    }

    fun update_tree_max_min(
        summary: &mut MarketSummary,
        tree: &Tree<PriceStoreElem>,
        side: u8,
    ) {
        // Because iteration is quite different depending on the side, split it up into two cases.
        if (side == SIDE_BUY) {
            summary.buyTreeMax = 0;
            let currNodeID = tree.max;
            while (currNodeID != 0) {
                let node = table::borrow(&tree.nodes, currNodeID);
                let size = vector::length(&node.elements);
                let i = size;
                while (i > 0) {
                    let elem = vector::borrow(&node.elements, i-1);
                    summary.buyTreeMax = elem.key;
                    return
                };
                currNodeID = node.prev;
            };
        } else {
            summary.sellTreeMin = 0;
            let currNodeID = tree.min;
            while (currNodeID != 0) {
                let node = table::borrow(&tree.nodes, currNodeID);
                let size = vector::length(&node.elements);
                let i = 0;
                while (i < size) {
                    let elem = vector::borrow(&node.elements, i);
                    summary.sellTreeMin = elem.key;
                    return
                };
                currNodeID = node.prev;
            };
        };
    }

    fun add_order_to_book<I, Q>(
        owner: &signer,
        accountKey: MarketAccountKey,
        marketAddr: address,
        behaviour: u8,
        side: u8,
        price: u64, // Fixedpoint value.
        qty: u64, // Fixedpoint value.
        clientOrderID: u32,
        marketBuyMaxCollateral: u64, // Should only be specified for market orders.
        book: &mut Orderbook<I, Q>,
        execs: &mut vector<ExecutionQueueEvent>,
    ): u32 acquires MarketSellTree, MarketBuyTree, MarketSellCache, MarketBuyCache, IndexingEventHandles {
        // Validate inputs.
        // <editor-fold defaultstate="collapsed" desc="Input Validation">
        utils::fp_round(qty, book.iDecimals, FP_NO_PRECISION_LOSS);
        utils::fp_round(price, book.qDecimals, FP_NO_PRECISION_LOSS);
        utils::fp_round(marketBuyMaxCollateral, book.qDecimals, FP_NO_PRECISION_LOSS);
        assert!(side == SIDE_BUY || side == SIDE_SELL, ERR_INVALID_SIDE);
        assert!(behaviour == BEHAVIOUR_IOC || behaviour == BEHAVIOUR_GTC || behaviour == BEHAVIOUR_FOK || behaviour == BEHAVIOUR_POST, ERR_INVALID_BEHAVIOUR);
        if (price == 0) {
            // Market orders can only have IOC or FOK behaviours.
            assert!(behaviour == BEHAVIOUR_IOC || behaviour == BEHAVIOUR_FOK, ERR_INVALID_BEHAVIOUR);
            // Max collateral amount must be defined correctly.
            if (side == SIDE_BUY) {
                assert!(marketBuyMaxCollateral > 0, ERR_INVALID_MAX_COLLATERAL_AMT);
            } else {
                assert!(marketBuyMaxCollateral == 0, ERR_INVALID_MAX_COLLATERAL_AMT);
            };
        } else {
            assert!(marketBuyMaxCollateral == 0, ERR_INVALID_MAX_COLLATERAL_AMT);
        };
        // </editor-fold>

        // Compute minAsk/maxBid.
        let minAsk = if (book.summary.sellCacheMin != 0) {
            book.summary.sellCacheMin
        } else {
            book.summary.sellTreeMin
        };
        let maxBid = if (book.summary.buyCacheMax != 0) {
            book.summary.buyCacheMax
        } else {
            book.summary.buyTreeMax
        };
        let crossesSpread = (
            price == 0 ||
            side == SIDE_SELL && price <= maxBid && maxBid != 0 ||
            side == SIDE_BUY && price >= minAsk && minAsk != 0
        );

        // Perform checks on order behaviour and cancel before trying to add to the book.
        // <editor-fold defaultstate="collapsed" desc="Order Behaviour Checks">
        if (behaviour == BEHAVIOUR_IOC && price != 0 && !crossesSpread) {
            // Cancel limit IOC orders that don't cross the spread because they won't execute.
            emit_finalized_event<I, Q>(marketAddr, accountKey, price, qty);
            return 0
        } else if (behaviour == BEHAVIOUR_POST && crossesSpread) {
            // Cancel POST orders that cross the spread because we can't guarantee that they will be makers.
            emit_finalized_event<I, Q>(marketAddr, accountKey, price, qty);
            return 0
        } else if (behaviour == BEHAVIOUR_FOK) {
            // Check to make sure a FOK order can be filled by orders on the book. Otherwise, cancel it.
            let remainingQty = qty;
            // First, check the cache.
            let cache = if (side == SIDE_SELL) {
                &borrow_global<MarketBuyCache<I, Q>>(marketAddr).cache
            } else {
                &borrow_global<MarketSellCache<I, Q>>(marketAddr).cache
            };
            // Inlined cache iteration.
            let i = vector::length(&cache.list);
            while (i > 0) {
                let cacheNode = vector::borrow(&cache.list, i - 1);
                if (side == SIDE_SELL && cacheNode.key < price || side == SIDE_BUY && cacheNode.key > price) {
                    // We've reached the limit price.
                    break
                };
                if (cacheNode.value.qty == 0) {
                    // Skip any prices that have no quantity (they are waiting for the crank to run before
                    // they are removed.
                    i = i - 1;
                    continue
                };
                if (remainingQty < cacheNode.value.qty) {
                    // Order qty is filled.
                    remainingQty = 0;
                    break
                };
                remainingQty = remainingQty - cacheNode.value.qty;
                i = i - 1;
            };
            // Then check tree if we still have qty.
            if (remainingQty > 0) {
                let (tree, it) = if (side == SIDE_SELL) {
                    let tree = &borrow_global<MarketBuyTree<I, Q>>(marketAddr).tree;
                    let it = tree_iterate(tree, SIDE_BUY);
                    (tree, it)
                } else {
                    let tree = &borrow_global<MarketSellTree<I, Q>>(marketAddr).tree;
                    let it = tree_iterate(tree, SIDE_SELL);
                    (tree, it)
                };
                while (it.pos.nodeID != 0) {
                    let (bookPrice, orderTreeElem) = tree_get_next(tree, &mut it);
                    // A 0 price means this is a market order and so will execute against anything.
                    if (
                        price != 0 &&
                            (side == SIDE_SELL && bookPrice < price)  ||
                            (side == SIDE_BUY && bookPrice > price)
                    ) {
                        // We've reached the limit price.
                        break
                    };
                    if (orderTreeElem.qty == 0) {
                        continue
                    };
                    if (remainingQty < orderTreeElem.qty) {
                        // Order qty is filled.
                        remainingQty = 0;
                        break
                    };
                    remainingQty = remainingQty - orderTreeElem.qty;
                };
                if (remainingQty > 0) {
                    // Cancel the order because we couldn't fill it.
                    emit_finalized_event<I, Q>(marketAddr, accountKey, price, qty);
                    return 0
                };
            };
        };
        // </editor-fold>

        // Create order object.
        assert!(table::contains(&book.marketAccounts, accountKey), ERR_NO_MARKET_ACCOUNT);
        let marketAccount = table::borrow_mut(&mut book.marketAccounts, accountKey);
        assert!(owns_account(owner, &accountKey, marketAccount), ERR_NOT_OWNER);
        let (buyCollateral, sellCollateral) = if (side == SIDE_BUY) {
            let quoteCoinAmt = if (price == 0) {
                utils::fp_convert(marketBuyMaxCollateral, coin::decimals<Q>(), FP_NO_PRECISION_LOSS)
            } else {
                utils::fp_convert(utils::fp_mul(price, qty, FP_NO_PRECISION_LOSS), coin::decimals<Q>(), FP_NO_PRECISION_LOSS)
            };
            (
                coin::extract<Q>(&mut marketAccount.quoteBalance, quoteCoinAmt),
                coin::zero<I>(),
            )
        } else {
            let instrumentCoinAmt = utils::fp_convert(qty, coin::decimals<I>(), FP_NO_PRECISION_LOSS);
            (
                coin::zero<Q>(),
                coin::extract(&mut marketAccount.instrumentBalance, instrumentCoinAmt),
            )
        };
        let orderID = get_or_create_order(&mut book.ordersTable);
        let order = table::borrow_mut(&mut book.ordersTable.objects, orderID);
        order.metadata = OrderMetadata{
            side,
            behaviour,
            price,
            originalQty: qty,
            unfilledQty: qty,
            takerCrankPendingQty: 0,
            clientOrderID,
            accountKey,
            ownerAddress: address_of(owner),
            marketBuyRemainingCollateral: marketBuyMaxCollateral,
        };
        coin::merge(&mut order.buyCollateral, buyCollateral);
        coin::merge(&mut order.sellCollateral, sellCollateral);
        // Get some side dependant variables.
        let canMaybeExecAgainstTree = can_maybe_execute_against_tree(&mut book.summary, side, price);
        let timestampSecs = timestamp::now_seconds();
        // Match against opposite side if the order crosses the spread.
        if (crossesSpread) {
            // If a taker, need to match against existing orders.
            // First load and match against the cache.
            // Then if needed, load tree and match order against that.
            if (side == SIDE_BUY) {
                if (book.summary.sellCacheQty > 0) {
                    let cache = &mut borrow_global_mut<MarketSellCache<I, Q>>(marketAddr).cache;
                    let qtyRemoved = match_against_cache(execs, cache, orderID, order, timestampSecs, book.iDecimals);
                    update_cache_size_and_qty(&mut book.summary, cache, qtyRemoved, 0);
                    update_cache_max_min(&mut book.summary, cache);
                };
                if (!no_qty_to_be_executed(order, 0) && canMaybeExecAgainstTree) {
                    let tree = &mut borrow_global_mut<MarketSellTree<I, Q>>(marketAddr).tree;
                    match_against_tree(execs, tree, orderID, order, timestampSecs, book.iDecimals);
                    update_tree_max_min(&mut book.summary, tree, SIDE_SELL);
                };
            } else if (side == SIDE_SELL) {
                if (book.summary.buyCacheQty > 0) {
                    let cache = &mut borrow_global_mut<MarketBuyCache<I, Q>>(marketAddr).cache;
                    let qtyRemoved = match_against_cache(execs, cache, orderID, order, timestampSecs, book.iDecimals);
                    update_cache_size_and_qty(&mut book.summary, cache, qtyRemoved, 0);
                    update_cache_max_min(&mut book.summary, cache);
                };
                if (!no_qty_to_be_executed(order, 0) && canMaybeExecAgainstTree) {
                    let tree= &mut borrow_global_mut<MarketBuyTree<I, Q>>(marketAddr).tree;
                    match_against_tree(execs, tree, orderID, order, timestampSecs, book.iDecimals);
                    update_tree_max_min(&mut book.summary, tree, SIDE_BUY);
                };
            };
        };
        let ordersTable = &mut book.ordersTable;
        let order = table::borrow_mut(&mut ordersTable.objects, orderID); // Reborrow.
        if (no_qty_to_be_executed(order, 0)) {
            // If the order is fully executed, no need to add it to the price store.
            return orderID
        };
        if (behaviour == BEHAVIOUR_IOC || price == 0) {
            // If the order is an IOC order or a market order, any remaining qty should be cancelled.
            order.metadata.unfilledQty = order.metadata.takerCrankPendingQty; // There should be no maker pending qty for this order.
            if (is_finalized(order)) {
                // Order is fully finalized. Emit finalize event and reuse order object.
                emit_finalized_event<I, Q>(
                    marketAddr,
                    order.metadata.accountKey,
                    order.metadata.price,
                    order.metadata.originalQty,
                );
                order.next = ordersTable.unusedStack;
                order.priceLevelID = 0;
                order.metadata = OrderMetadata {
                    side: 0,
                    behaviour: 0,
                    price: 0,
                    originalQty: 0,
                    unfilledQty: 0,
                    takerCrankPendingQty: 0,
                    clientOrderID: 0,
                    accountKey: sentinal_market_account_key(),
                    ownerAddress: @0,
                    marketBuyRemainingCollateral: 0,
                };
                ordersTable.unusedStack = orderID;
                return 0
            };
            return orderID
        };
        // Otherwise, order is added to the price store.
        let remainingQty = order.metadata.unfilledQty - order.metadata.takerCrankPendingQty;
        let priceLevelID = if (should_insert_in_cache(&book.summary, book.maxCacheSize, side, price)) {
            // Order price will go into the cache.
            let cache = if (side == SIDE_BUY) {
                &mut borrow_global_mut<MarketBuyCache<I, Q>>(marketAddr).cache
            } else {
                &mut borrow_global_mut<MarketSellCache<I, Q>>(marketAddr).cache
            };
            let priceLevelID = add_price_qty_to_cache(cache, &mut book.priceLevelsTable, price, remainingQty);
            update_cache_size_and_qty(&mut book.summary, cache, 0, remainingQty);
            update_cache_max_min(&mut book.summary, cache);
            priceLevelID
        } else {
            // Order price will go into the tree.
            let tree = if (side == SIDE_BUY) {
                &mut borrow_global_mut<MarketBuyTree<I, Q>>(marketAddr).tree
            } else {
                &mut borrow_global_mut<MarketSellTree<I, Q>>(marketAddr).tree
            };
            let priceLevelID = add_price_qty_to_tree(tree, &mut book.priceLevelsTable, price, remainingQty);
            update_tree_max_min(&mut book.summary, tree, side);
            priceLevelID
        };
        // Add order to the corresponding PriceLevel object.
        let order = table::borrow_mut(&mut book.ordersTable.objects, orderID); // Reborrow.
        let priceLevel = table::borrow_mut(&mut book.priceLevelsTable.objects, priceLevelID);
        list_push(&mut priceLevel.orders, PriceLevelOrder {
            id: orderID,
            qty: remainingQty,
        });
        order.priceLevelID = priceLevelID;
        orderID
    }

    fun cancel_order<I, Q>(
        owner: &signer,
        orderID: u32,
    ) acquires FerumInfo, Orderbook, MarketBuyTree, MarketBuyCache, MarketSellTree, MarketSellCache, IndexingEventHandles {
        let marketAddr = get_market_addr<I, Q>();
        let book = borrow_global_mut<Orderbook<I, Q>>(marketAddr);
        assert!(table::contains(&book.ordersTable.objects, orderID), ERR_UNKNOWN_ORDER);
        let order = table::borrow(&book.ordersTable.objects, orderID);
        assert!(order.metadata.ownerAddress != @0, ERR_UNKNOWN_ORDER);
        let side = order.metadata.side;
        let price = order.metadata.price;
        // If the order is cancelable, then there is some amount of unfilledQty that is not pending a crank turn. Check
        // for pending taker qty here. Maker qty is checked for when we try to remove from the price level.
        assert!(order.metadata.unfilledQty > order.metadata.takerCrankPendingQty, ERR_ORDER_EXECUTED_BUT_IS_PENDING_CRANK);
        // Also make sure the order has a price level associated with it.
        assert!(order.priceLevelID != 0, ERR_UNKNOWN_ORDER);
        // Remove order from price store and level.
        let qtyCancelled = remove_order_from_price_store_and_level<I, Q>(marketAddr, &mut book.priceLevelsTable, &mut book.summary, orderID, side, price);
        // Update order.
        let book = borrow_global_mut<Orderbook<I, Q>>(marketAddr); // Reborrow.
        let ordersTable = &mut book.ordersTable;
        let order = table::borrow_mut(&mut ordersTable.objects, orderID);
        let marketAccount = table::borrow_mut(&mut book.marketAccounts, order.metadata.accountKey);
        // Better to do this check above but doing it here to save a borrow call.
        assert!(owns_account(owner, &order.metadata.accountKey, marketAccount), ERR_NOT_OWNER);
        order.metadata.unfilledQty = order.metadata.unfilledQty - qtyCancelled;
        // Release collateral.
        if (order.metadata.side == SIDE_BUY) {
            let quoteDecimals = coin::decimals<Q>();
            let quoteAmt = utils::fp_convert(utils::fp_mul(price, qtyCancelled, FP_NO_PRECISION_LOSS), quoteDecimals, FP_NO_PRECISION_LOSS);
            let quoteCoinAmt = coin::extract(&mut order.buyCollateral, quoteAmt);
            coin::merge(&mut marketAccount.quoteBalance, quoteCoinAmt);
        } else {
            let instrumentDecimals = coin::decimals<I>();
            let instrumentAmt = utils::fp_convert(qtyCancelled, instrumentDecimals, FP_NO_PRECISION_LOSS);
            let instrumentCoinAmt = coin::extract(&mut order.sellCollateral, instrumentAmt);
            coin::merge(&mut marketAccount.instrumentBalance, instrumentCoinAmt);
        };
        // Shouldn't need to worry about market orders because they will never be in the book.
        if (is_finalized(order)) {
            // Order is fully finalized. Emit finalize event and reuse order object.
            emit_finalized_event<I, Q>(
                marketAddr,
                order.metadata.accountKey,
                order.metadata.price,
                order.metadata.originalQty,
            );
            order.next = ordersTable.unusedStack;
            order.priceLevelID = 0;
            order.metadata = OrderMetadata {
                side: 0,
                behaviour: 0,
                price: 0,
                originalQty: 0,
                unfilledQty: 0,
                takerCrankPendingQty: 0,
                clientOrderID: 0,
                ownerAddress: @0,
                accountKey: sentinal_market_account_key(),
                marketBuyRemainingCollateral: 0,
            };
            ordersTable.unusedStack = orderID;
        };
    }

    inline fun emit_finalized_event<I, Q>(
        marketAddr: address,
        accountKey: MarketAccountKey,
        price: u64,
        originalQty: u64,
    ) acquires IndexingEventHandles {
        let finalizeEventHandle = &mut borrow_global_mut<IndexingEventHandles<I, Q>>(marketAddr).finalizations;
        emit_event(finalizeEventHandle, IndexingFinalizeEvent {
            accountKey,
            price,
            originalQty,
            timestampSecs: timestamp::now_seconds(),
        });
    }

    fun remove_order_from_price_store_and_level<I, Q>(
        marketAddr: address,
        priceLevelsTable: &mut PriceLevelReuseTable,
        summary: &mut MarketSummary,
        orderID: u32,
        orderSide: u8,
        orderPrice: u64,
    ): u64 acquires MarketBuyTree, MarketSellTree, MarketBuyCache, MarketSellCache {
        if (is_price_store_elem_in_cache(summary, orderSide, orderPrice)) {
            // The price is in the cache.
            let cache = if (orderSide == SIDE_BUY) {
                &mut borrow_global_mut<MarketBuyCache<I, Q>>(marketAddr).cache
            } else {
                &mut borrow_global_mut<MarketSellCache<I, Q>>(marketAddr).cache
            };
            let res = cache_find(cache, orderPrice);
            assert!(vector::length(&res) > 0, ERR_PRICE_STORE_ELEM_NOT_FOUND);
            let idx = vector::pop_back(&mut res);
            let priceStoreElem = &vector::borrow_mut(&mut cache.list, idx).value;
            let qtyRemoved = remove_order_from_price_level(
                priceLevelsTable,
                priceStoreElem.priceLevelID,
                orderID,
                priceStoreElem.makerCrankPendingQty,
            );
            remove_price_qty_from_cache(
                cache,
                orderPrice,
                qtyRemoved,
                false,
                res,
            );
            update_cache_size_and_qty(summary, cache, qtyRemoved, 0);
            update_cache_max_min(summary, cache);
            qtyRemoved
        } else {
            // The price is in the tree.
            let tree = if (orderSide == SIDE_BUY) {
                &mut borrow_global_mut<MarketBuyTree<I, Q>>(marketAddr).tree
            } else {
                &mut borrow_global_mut<MarketSellTree<I, Q>>(marketAddr).tree
            };
            let pos = tree_find(tree, orderPrice);
            assert!(pos.nodeID != 0, ERR_PRICE_STORE_ELEM_NOT_FOUND);
            let (_, priceStoreElem) = tree_get_mut(tree, &pos);
            let qtyRemoved = remove_order_from_price_level(
                priceLevelsTable,
                priceStoreElem.priceLevelID,
                orderID,
                priceStoreElem.makerCrankPendingQty,
            );
            remove_price_qty_from_tree(
                tree,
                orderPrice,
                qtyRemoved,
                false,
                vector[pos],
            );
            update_tree_max_min(summary, tree, orderSide);
            qtyRemoved
        }
    }

    // Adds the order's price to the cache and returns the ID of the PriceLevel.
    fun add_price_qty_to_cache(
        cache: &mut Cache<PriceStoreElem>,
        priceLevels: &mut PriceLevelReuseTable,
        price: u64,
        qty: u64,
    ): u16 {
        // Get the price level ID from the node.
        let res = cache_find(cache, price);
        if (vector::length(&res) == 1) {
            let idx = vector::pop_back(&mut res);
            let cacheNode = vector::borrow_mut(&mut cache.list, idx);
            cacheNode.value.qty = cacheNode.value.qty + qty;
            cacheNode.value.priceLevelID
        } else {
            // If it doesn't exist, create and add it to the cache.
            let priceLevelID = get_or_create_price_level(priceLevels);
            cache_insert(cache, price, PriceStoreElem {
                qty,
                makerCrankPendingQty: 0,
                priceLevelID,
            });
            priceLevelID
        }
    }

    // Adds the order's price to the tree and returns the ID of the PriceLevel.
    fun add_price_qty_to_tree(
        tree: &mut Tree<PriceStoreElem>,
        priceLevels: &mut PriceLevelReuseTable,
        price: u64,
        qty: u64,
    ): u16 {
        // First try and find to find the price in the tree.
        let pos = tree_find(tree, price);
        if (pos.nodeID != 0) {
            // The price is in the tree.
            let (_, priceStoreElem) = tree_get_mut(tree, &pos);
            priceStoreElem.qty = priceStoreElem.qty + qty;
            return priceStoreElem.priceLevelID
        };
        // Otherwise, we need to insert the price into the tree.
        let priceLevelID = get_or_create_price_level(priceLevels);
        tree_insert(tree, price, PriceStoreElem {
            qty,
            makerCrankPendingQty: 0,
            priceLevelID,
        });
        priceLevelID
    }

    // Removes the qty from the tree for the given price level.
    fun remove_price_qty_from_tree(
        tree: &mut Tree<PriceStoreElem>,
        price: u64, // Fixedpoint value.
        qty: u64, // Fixedpoint value.
        crank: bool, // Wether or not this function is being called from the crank.
        treePosHint: vector<TreePosition>,
    ) {
        let pos = if (vector::length(&treePosHint) > 0) {
            vector::borrow(&treePosHint, 0)
        } else {
            let pos = tree_find(tree, price);
            assert!(pos.nodeID != 0, ERR_TREE_ELEM_DOES_NOT_EXIST);
            &pos
        };
        let (_, priceStoreElem) = tree_get_mut(tree, pos);
        if (crank) {
            priceStoreElem.makerCrankPendingQty = priceStoreElem.makerCrankPendingQty - qty;
        } else {
            priceStoreElem.qty = priceStoreElem.qty - qty;
        };
        if (priceStoreElem.qty == 0 && priceStoreElem.makerCrankPendingQty == 0) {
            // Only remove the price from the price store if qty is 0 and there is no pending crank qty.
            tree_delete(tree, price);
        };
    }

    // Removes the qty from the cache for the given price level.
    fun remove_price_qty_from_cache(
        cache: &mut Cache<PriceStoreElem>,
        price: u64, // Fixedpoint value.
        qty: u64, // Fixedpoint value.
        crank: bool, // Wether or not this function is being called from the crank.
        cacheIdxHint: vector<u64>,
    ) {
        let idx = if (vector::length(&cacheIdxHint) == 1) {
            *vector::borrow(&cacheIdxHint, 0)
        } else {
            let res = cache_find(cache, price);
            assert!(vector::length(&res) == 1, ERR_PRICE_STORE_ELEM_NOT_FOUND);
            vector::pop_back(&mut res)
        };
        let priceStoreElem = &mut vector::borrow_mut(&mut cache.list, idx).value;
        if (crank) {
            priceStoreElem.makerCrankPendingQty = priceStoreElem.makerCrankPendingQty - qty;
        } else {
            priceStoreElem.qty = priceStoreElem.qty - qty;
        };
        if (priceStoreElem.qty == 0 && priceStoreElem.makerCrankPendingQty == 0) {
            // Only remove the price from the price store if qty is 0 and there is no pending crank qty.
            cache_remove(cache, price);
        };
    }

    // Removes the order from the price level, returning the amount of qty that was removed.
    // Aborts if the order already has been executed.
    fun remove_order_from_price_level(
        priceLevels: &mut PriceLevelReuseTable,
        priceLevelID: u16,
        orderID: u32,
        pendingMakerCrankQty: u64,
    ): u64 {
        let priceLevel = table::borrow_mut(&mut priceLevels.objects, priceLevelID);
        let list = &mut priceLevel.orders;
        let qtyRemoved = 0;
        let it = list_iterate(list);
        while (it.nodeID != 0) {
            let elem = list_get_mut(list, &mut it);
            if (elem.id == orderID) {
                if (pendingMakerCrankQty >= elem.qty) {
                    // We can't cancel this order because it's entire quantity has already been executed and is just
                    // waiting for the crank to turn.
                    abort ERR_ORDER_EXECUTED_BUT_IS_PENDING_CRANK
                };
                qtyRemoved = elem.qty - pendingMakerCrankQty;
                elem.qty = elem.qty - qtyRemoved;
                if (elem.qty == 0) {
                    // If we've used all the qty of the price store elem, remove it.
                    list_remove(list, it);
                };
                break
            };
            if (pendingMakerCrankQty > elem.qty) {
                pendingMakerCrankQty = pendingMakerCrankQty - elem.qty;
            } else {
                pendingMakerCrankQty = 0;
            };
            list_next(list, &mut it);
        };
        assert!(qtyRemoved > 0, ERR_PRICE_STORE_ELEM_NOT_FOUND);
        // Remove PriceLevel if needed.
        if (list.length == 0) {
            priceLevel.next = priceLevels.unusedStack;
            priceLevels.unusedStack = priceLevelID;
        };
        qtyRemoved
    }

    fun match_against_cache<I, Q>(
        execs: &mut vector<ExecutionQueueEvent>,
        cache: &mut Cache<PriceStoreElem>,
        orderID: u32,
        order: &mut Order<I, Q>,
        timestampSecs: u64,
        instrumentDecimals: u8,
    ): u64 {
        let smallestInstrumentAmt = utils::exp64(DECIMAL_PLACES - instrumentDecimals);
        let size = vector::length(&cache.list);
        let i = size;
        let qtyExecuted = 0;
        // Inlined iteration.
        while (i > 0) {
            let cacheNode = vector::borrow_mut(&mut cache.list, i - 1);
            let bookPrice = cacheNode.key;
            if (
                order.metadata.price != 0 && (
                    (order.metadata.side == SIDE_BUY && bookPrice > order.metadata.price) ||
                    (order.metadata.side == SIDE_SELL && bookPrice < order.metadata.price)
                )
            ) {
                // We've reached the limit price.
                break
            };
            if (cacheNode.value.qty == 0) {
                // Skip any prices with 0 qty (these prices are waiting for crank to turn).
                i = i - 1;
                continue
            };
            let remainingQty = order.metadata.unfilledQty - order.metadata.takerCrankPendingQty;
            let fillQty = if (cacheNode.value.qty > remainingQty) {
                remainingQty
            } else {
                cacheNode.value.qty
            };
            if (order.metadata.price == 0 && order.metadata.side == SIDE_BUY) {
                let usedBuyCollateral = utils::fp_mul(fillQty, bookPrice, FP_NO_PRECISION_LOSS);
                if (order.metadata.marketBuyRemainingCollateral < usedBuyCollateral) {
                    // Need to consider remaining buy collateral for market buy orders. If the remaining buy collateral is
                    // not enough to cover the fillQty, clamp fillQty to what the remaining buy collateral can cover.
                    fillQty = utils::fp_round(
                        utils::fp_div(order.metadata.marketBuyRemainingCollateral, bookPrice, FP_TRUNC),
                        instrumentDecimals,
                        FP_TRUNC,
                    );
                    if (fillQty == 0) {
                        break
                    };
                    usedBuyCollateral = utils::fp_mul(fillQty, bookPrice, FP_NO_PRECISION_LOSS);
                };
                // Update the remaining buy collateral.
                order.metadata.marketBuyRemainingCollateral = order.metadata.marketBuyRemainingCollateral - usedBuyCollateral;
                // Detect if the amount of collateral has gotten so small that no more executions at the current price
                // or higher are possible. In this case, set marketBuyRemainingCollateral to 0 because there is no remaining
                // collateral left for this market order.
                if (order.metadata.marketBuyRemainingCollateral < utils::fp_mul(smallestInstrumentAmt, bookPrice, FP_ROUND_UP)) {
                    order.metadata.marketBuyRemainingCollateral = 0;
                };
            };
            qtyExecuted = qtyExecuted + fillQty;
            order.metadata.takerCrankPendingQty = order.metadata.takerCrankPendingQty + fillQty;
            cacheNode.value.makerCrankPendingQty = cacheNode.value.makerCrankPendingQty + fillQty;
            // Create exec event.
            vector::push_back(execs, ExecutionQueueEvent {
                qty: fillQty,
                priceLevelID: cacheNode.value.priceLevelID,
                timestampSecs,
                takerOrderID: orderID,
            });
            // Update the price qty in the cache.
            remove_price_qty_from_cache(
                cache,
                bookPrice,
                fillQty,
                false,
                vector[i-1],
            );
            // If the order is finalized, break.
            if (no_qty_to_be_executed(order, 0)) {
                break
            };
            i = i - 1;
        };
        qtyExecuted
    }

    struct DeferredQtyRemovals has drop {
        position: TreePosition,
        qty: u64,
    }

    fun match_against_tree<I, Q>(
        execs: &mut vector<ExecutionQueueEvent>,
        tree: &mut Tree<PriceStoreElem>,
        orderID: u32,
        order: &mut Order<I, Q>,
        timestampSecs: u64,
        instrumentDecimals: u8,
    ) {
        // Qty removals from the tree need to be deferred because remove qty can result in the remove of a node and
        // that messes up iteration.
        let qtysToRemove = vector[];
        let smallestInstrumentAmt = utils::exp64(DECIMAL_PLACES - instrumentDecimals);
        let it = tree_iterate(tree, if (order.metadata.side == SIDE_BUY) {
            SIDE_SELL
        } else {
            SIDE_BUY
        });
        while (it.pos.nodeID != 0) {
            let currPos = it.pos;
            let (bookPrice, orderTreeElem) = tree_get_next_mut(tree, &mut it);
            if (
                order.metadata.price != 0 && (
                    (order.metadata.side == SIDE_BUY && bookPrice > order.metadata.price) ||
                    (order.metadata.side == SIDE_SELL && bookPrice < order.metadata.price)
                )
            ) {
                // We've reached the limit price.
                break
            };
            if (orderTreeElem.qty == 0) {
                // Skip price levels that have no qty.
                continue
            };
            let remainingQty = order.metadata.unfilledQty - order.metadata.takerCrankPendingQty;
            let fillQty = if (orderTreeElem.qty > remainingQty) {
                remainingQty
            } else {
                orderTreeElem.qty
            };
            if (order.metadata.price == 0 && order.metadata.side == SIDE_BUY) {
                let usedBuyCollateral = utils::fp_mul(fillQty, bookPrice, FP_NO_PRECISION_LOSS);
                if (order.metadata.marketBuyRemainingCollateral < usedBuyCollateral) {
                    // Need to consider remaining buy collateral for market buy orders. If the remaining buy collateral is
                    // not enough to cover the fillQty, clamp fillQty to what the remaining buy collateral can cover.
                    fillQty = utils::fp_round(
                        utils::fp_div(order.metadata.marketBuyRemainingCollateral, bookPrice, FP_TRUNC),
                        instrumentDecimals,
                        FP_TRUNC,
                    );
                    if (fillQty == 0) {
                        break
                    };
                    usedBuyCollateral = utils::fp_mul(fillQty, bookPrice, FP_NO_PRECISION_LOSS);
                };
                // Update the remaining buy collateral.
                order.metadata.marketBuyRemainingCollateral = order.metadata.marketBuyRemainingCollateral - usedBuyCollateral;
                // Detect if the amount of collateral has gotten so small that no more executions at the current price
                // or higher are possible. In this case, set marketBuyRemainingCollateral to 0 because there is no remaining
                // collateral left for this market order.
                if (order.metadata.marketBuyRemainingCollateral < utils::fp_mul(smallestInstrumentAmt, bookPrice, FP_ROUND_UP)) {
                    order.metadata.marketBuyRemainingCollateral = 0;
                };
            };
            order.metadata.takerCrankPendingQty = order.metadata.takerCrankPendingQty + fillQty;
            orderTreeElem.makerCrankPendingQty = orderTreeElem.makerCrankPendingQty + fillQty;
            // Create exec event.
            vector::push_back(execs, ExecutionQueueEvent {
                qty: fillQty,
                priceLevelID: orderTreeElem.priceLevelID,
                timestampSecs,
                takerOrderID: orderID,
            });
            // Defer update for price qty in the tree.
            vector::push_back(&mut qtysToRemove, DeferredQtyRemovals{
                position: currPos,
                qty: fillQty,
            });
            // If the order is finalized, break.
            if (no_qty_to_be_executed(order, 0)) {
                break
            };
        };

        // Remove qtys from tree.
        let i = 0;
        let size = vector::length(&qtysToRemove);
        while (i < size) {
            let DeferredQtyRemovals {
                position,
                qty,
            } = vector::pop_back(&mut qtysToRemove);
            remove_price_qty_from_tree(
                tree,
                order.metadata.price,
                qty,
                false,
                vector[position],
            );
            i = i + 1;
        };
    }

    inline fun get_or_create_order<I, Q>(table: &mut OrderReuseTable<I, Q>): u32 {
        if (table.unusedStack == 0) {
            prealloc_orders(table, 1)
        };
        let orderID = table.unusedStack;
        let order = table::borrow_mut(&mut table.objects, orderID);
        table.unusedStack = order.next;
        order.next = 0;
        orderID
    }

    inline fun get_or_create_price_level(table: &mut PriceLevelReuseTable): u16 {
        if (table.unusedStack == 0) {
            prealloc_price_levels(table, 1)
        };
        let priceLevelID = table.unusedStack;
        let priceLevel = table::borrow_mut(&mut table.objects, priceLevelID);
        table.unusedStack = priceLevel.next;
        priceLevel.next = 0;
        priceLevelID
    }

    inline fun validate_coins<I, Q>(): (u8, u8) {
        let iDecimals = coin::decimals<I>();
        let qDecimals = coin::decimals<Q>();
        assert!(coin::is_coin_initialized<Q>(), ERR_COIN_UNINITIALIZED);
        assert!(qDecimals <= MAX_DECIMALS, ERR_COIN_EXCEEDS_MAX_SUPPORTED_DECIMALS);
        assert!(coin::is_coin_initialized<I>(), ERR_COIN_UNINITIALIZED);
        assert!(iDecimals <= MAX_DECIMALS, ERR_COIN_EXCEEDS_MAX_SUPPORTED_DECIMALS);
        (iDecimals, qDecimals)
    }

    // Helper to return if a price is executable against the tree. Note that this isn't perfect as
    // buyTreeMax/sellTreeMin might point to a price level whose entire qty might be used and pending a crank turn.
    // If this returns false, than an order with `price` can't execute against the tree. Otherwise, an order with
    // `price` might execute against the tree.
    inline fun can_maybe_execute_against_tree(
        summary: &MarketSummary,
        orderSide: u8,
        price: u64, // Fixedpoint value.
    ): bool {
        if (orderSide == SIDE_SELL) {
            // Will execute against buy tree.
            price == 0 || (summary.buyTreeMax != 0 && price <= summary.buyTreeMax)
        } else {
            // Will execute against sell tree.
            price == 0 || (summary.sellTreeMin != 0 && price >= summary.sellTreeMin)
        }
    }

    // Returns true if a new price should be inserted into the cache.
    inline fun should_insert_in_cache(
        summary: &MarketSummary,
        maxCacheSize: u8,
        side: u8,
        price: u64, // Fixedpoint value.
    ): bool {
        if (side == SIDE_BUY) {
            summary.buyCacheSize < maxCacheSize && (summary.buyTreeMax == 0 || price > summary.buyTreeMax) ||
                summary.buyCacheSize >= maxCacheSize && (summary.buyCacheMin != 0 && price >= summary.buyCacheMin)
        } else {
            summary.sellCacheSize < maxCacheSize && (summary.sellTreeMin == 0 || price < summary.sellTreeMin) ||
                summary.sellCacheSize >= maxCacheSize && (summary.sellCacheMax != 0 && price <= summary.sellCacheMax)
        }
    }

    // Given a price that already exists in the price store, returns true if the price is in the cache.
    inline fun is_price_store_elem_in_cache(
        summary: &MarketSummary,
        side: u8,
        price: u64, // Fixedpoint value.
    ): bool {
        if (side == SIDE_BUY) {
            summary.buyTreeMax == 0 || price > summary.buyTreeMax ||
                (summary.buyCacheMin != 0 && price >= summary.buyCacheMin)
        } else {
            summary.sellTreeMin == 0 || price < summary.sellTreeMin ||
                (summary.sellCacheMax != 0 && price <= summary.sellCacheMax)
        }
    }

    // Returns true if the order can no longer be executed.
    inline fun no_qty_to_be_executed<I, Q>(
        order: &Order<I, Q>,
        makerPendingCrankQty: u64, // Fixedpoint value.
    ): bool {
        let marketBuyAndCannotExecute = order.metadata.price == 0 &&
            order.metadata.side == SIDE_BUY &&
            order.metadata.marketBuyRemainingCollateral == 0;
        marketBuyAndCannotExecute || order.metadata.unfilledQty - order.metadata.takerCrankPendingQty - makerPendingCrankQty == 0
    }

    // Returns true if the order is finalized. To be finalized means the order can no longer be executed any more and
    // there is no pending qty remaining.
    inline fun is_finalized<I, Q>(
        order: &Order<I, Q>,
    ): bool {
        let marketBuyAndCannotExecute = order.metadata.price == 0 &&
            order.metadata.side == SIDE_BUY &&
            order.metadata.marketBuyRemainingCollateral == 0;
        (marketBuyAndCannotExecute || order.metadata.unfilledQty == 0) && order.metadata.takerCrankPendingQty == 0
    }

    // Returns true if the signer is able to perform mutative actions on an account and for the orders that the account
    // placed. Only the protocol or the address that created the account should be allowed.
    inline fun owns_account<I, Q>(
        owner: &signer,
        accountKey: &MarketAccountKey,
        marketAccount: &MarketAccount<I, Q>,
    ): bool {
        let ownerAddr = address_of(owner);
        ownerAddr == marketAccount.ownerAddress || ownerAddr == accountKey.protocolAddress
    }

    inline fun sentinal_market_account_key(): MarketAccountKey {
        MarketAccountKey {
            protocolAddress: @0,
            userAddress: @0,
        }
    }

    fun account_key_from_identifier(id: AccountIdentifier): MarketAccountKey {
        let (protocolAddress, userAddress) = platform::get_addresses(&id);
        MarketAccountKey {
            protocolAddress,
            userAddress,
        }
    }

    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="Market tests">

    // <editor-fold defaultstate="collapsed" desc="POST order tests">

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    fun test_market_post_order_cancelled_sell(aptos: &signer, ferum: &signer, user: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that a post buy order is cancelled when it crosses the spread.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 2);

        // Setup.
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 5000000000, 30000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 6000000000, 30000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 8000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 9000000000, 30000000000);
        let takerID = add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_POST, 5500000000, 100000000000);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.9 qty: 3, crankQty: 0)"),
            s(b"(0.8 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
            s(b"(0.6 qty: 3, crankQty: 0)"),
            s(b"(0.5 qty: 3, crankQty: 0)"),
        ]);
        assert_exec_events<FMA, FMB>(marketAddr, vector[]);
        assert!(takerID == 0, 0);
    }

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    fun test_market_post_order_cancelled_empty_cache_sell(aptos: &signer, ferum: &signer, user: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that a post buy order is cancelled when it cross the spread.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 2);

        // Setup.
        let orderID1 = add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 5000000000, 30000000000);
        let orderID2 = add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 6000000000, 30000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 8000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 9000000000, 30000000000);
        cancel_order<FMA, FMB>(user, orderID1);
        cancel_order<FMA, FMB>(user, orderID2);
        let takerID = add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_POST, 8500000000, 100000000000);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.9 qty: 3, crankQty: 0)"),
            s(b"(0.8 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
        ]);
        assert_exec_events<FMA, FMB>(marketAddr, vector[]);
        assert!(takerID == 0, 0);
    }

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    fun test_market_post_order_sell(aptos: &signer, ferum: &signer, user: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that a post buy order can be added to the book.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 2);

        // Setup.
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 5000000000, 30000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 6000000000, 30000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 8000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 9000000000, 30000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_POST, 4500000000, 10000000000);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.9 qty: 3, crankQty: 0)"),
            s(b"(0.8 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
            s(b"(0.6 qty: 3, crankQty: 0)"),
            s(b"(0.5 qty: 3, crankQty: 0)"),
        ]);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
        ], vector[
            s(b"(0.45 qty: 1, crankQty: 0)"),
        ]);
        assert_exec_events<FMA, FMB>(marketAddr, vector[]);
    }

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    fun test_market_post_order_cancelled_buy(aptos: &signer, ferum: &signer, user: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that a post sell order is cancelled when it crosses the spread.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 2);

        // Setup.
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 9000000000, 30000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 8000000000, 30000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 6000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 5000000000, 30000000000);
        let takerID = add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_POST, 8500000000, 100000000000);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.5 qty: 3, crankQty: 0)"),
            s(b"(0.6 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
            s(b"(0.8 qty: 3, crankQty: 0)"),
            s(b"(0.9 qty: 3, crankQty: 0)"),
        ]);
        assert_exec_events<FMA, FMB>(marketAddr, vector[]);
        assert!(takerID == 0, 0);
    }

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    fun test_market_post_order_cancelled_empty_cache_buy(aptos: &signer, ferum: &signer, user: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that a post sell order is cancelled when it crosses the spread.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 2);

        // Setup.
        let orderID1 = add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 9000000000, 30000000000);
        let orderID2 = add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 8000000000, 30000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 6000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 5000000000, 30000000000);
        cancel_order<FMA, FMB>(user, orderID1);
        cancel_order<FMA, FMB>(user, orderID2);
        let takerID = add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_POST, 6500000000, 100000000000);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.5 qty: 3, crankQty: 0)"),
            s(b"(0.6 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
        ]);
        assert_exec_events<FMA, FMB>(marketAddr, vector[]);
        assert!(takerID == 0, 0);
    }

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    fun test_market_post_order_buy(aptos: &signer, ferum: &signer, user: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that a post sell order can be added to the book.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 2);

        // Setup.
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 9000000000, 30000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 8000000000, 30000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 6000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 5000000000, 30000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_POST, 9500000000, 120000000000);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.5 qty: 3, crankQty: 0)"),
            s(b"(0.6 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
            s(b"(0.8 qty: 3, crankQty: 0)"),
            s(b"(0.9 qty: 3, crankQty: 0)"),
        ]);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[
        ], vector[
            s(b"(0.95 qty: 12, crankQty: 0)"),
        ]);
        assert_exec_events<FMA, FMB>(marketAddr, vector[]);
    }

    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="IOC order tests">

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    fun test_market_ioc_order_cancelled_sell(aptos: &signer, ferum: &signer, user: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that an ioc buy order is cancelled when it can't execute.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 2);

        // Setup.
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 5000000000, 30000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 6000000000, 30000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 8000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 9000000000, 30000000000);
        let takerID = add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_IOC, 4500000000, 100000000000);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.9 qty: 3, crankQty: 0)"),
            s(b"(0.8 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
            s(b"(0.6 qty: 3, crankQty: 0)"),
            s(b"(0.5 qty: 3, crankQty: 0)"),
        ]);
        assert_exec_events<FMA, FMB>(marketAddr, vector[]);
        assert!(takerID == 0, 0);
    }

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    fun test_market_ioc_order_cancelled_empty_cache_sell(aptos: &signer, ferum: &signer, user: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that an ioc buy order is cancelled when it can't execute.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 2);

        // Setup.
        let orderID1 = add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 5000000000, 30000000000);
        let orderID2 = add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 6000000000, 30000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 8000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 9000000000, 30000000000);
        cancel_order<FMA, FMB>(user, orderID1);
        cancel_order<FMA, FMB>(user, orderID2);
        let takerID = add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_IOC, 5500000000, 100000000000);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.9 qty: 3, crankQty: 0)"),
            s(b"(0.8 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
        ]);
        assert_exec_events<FMA, FMB>(marketAddr, vector[]);
        assert!(takerID == 0, 0);
    }

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    fun test_market_ioc_order_partial_fill_sell(aptos: &signer, ferum: &signer, user: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that an ioc buy order can be partially filled.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 2);

        // Setup.
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 5000000000, 30000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 6000000000, 30000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 8000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 9000000000, 30000000000);
        let takerID = add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_IOC, 7500000000, 120000000000);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.9 qty: 3, crankQty: 0)"),
            s(b"(0.8 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 0, crankQty: 5)"),
        ], vector[
            s(b"(0.6 qty: 0, crankQty: 3)"),
            s(b"(0.5 qty: 0, crankQty: 3)"),
        ]);
        assert_exec_events<FMA, FMB>(marketAddr, vector[
            ExecEventInfo {
                qty: 30000000000,
                takerOrderID: takerID,
                price: 5000000000,
            },
            ExecEventInfo {
                qty: 30000000000,
                takerOrderID: takerID,
                price: 6000000000,
            },
            ExecEventInfo {
                qty: 50000000000,
                takerOrderID: takerID,
                price: 7000000000,
            },
        ]);
        assert_order_qtys<FMA, FMB>(marketAddr, takerID, 110000000000, 110000000000, 0);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert_order_used(book, takerID);
    }

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    fun test_market_ioc_order_cancelled_buy(aptos: &signer, ferum: &signer, user: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that an ioc sell order is cancelled when it can't execute.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 2);

        // Setup.
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 9000000000, 30000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 8000000000, 30000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 6000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 5000000000, 30000000000);
        let takerID = add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_IOC, 9500000000, 100000000000);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.5 qty: 3, crankQty: 0)"),
            s(b"(0.6 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
            s(b"(0.8 qty: 3, crankQty: 0)"),
            s(b"(0.9 qty: 3, crankQty: 0)"),
        ]);
        assert_exec_events<FMA, FMB>(marketAddr, vector[]);
        assert!(takerID == 0, 0);
    }

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    fun test_market_ioc_order_cancelled_empty_cache_buy(aptos: &signer, ferum: &signer, user: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that an ioc sell order is cancelled when it can't execute.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 2);

        // Setup.
        let orderID1 = add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 9000000000, 30000000000);
        let orderID2 = add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 8000000000, 30000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 6000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 5000000000, 30000000000);
        cancel_order<FMA, FMB>(user, orderID1);
        cancel_order<FMA, FMB>(user, orderID2);
        let takerID = add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_IOC, 7500000000, 100000000000);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.5 qty: 3, crankQty: 0)"),
            s(b"(0.6 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
        ]);
        assert_exec_events<FMA, FMB>(marketAddr, vector[]);
        assert!(takerID == 0, 0);
    }

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    fun test_market_ioc_order_partial_fill_buy(aptos: &signer, ferum: &signer, user: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that an ioc sell order can be partially filled.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 2);

        // Setup.
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 9000000000, 30000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 8000000000, 30000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 6000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 5000000000, 30000000000);
        let takerID = add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_IOC,6500000000, 120000000000);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.5 qty: 3, crankQty: 0)"),
            s(b"(0.6 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 0, crankQty: 5)"),
        ], vector[
            s(b"(0.8 qty: 0, crankQty: 3)"),
            s(b"(0.9 qty: 0, crankQty: 3)"),
        ]);
        assert_exec_events<FMA, FMB>(marketAddr, vector[
            ExecEventInfo {
                qty: 30000000000,
                takerOrderID: takerID,
                price: 9000000000,
            },
            ExecEventInfo {
                qty: 30000000000,
                takerOrderID: takerID,
                price: 8000000000,
            },
            ExecEventInfo {
                qty: 50000000000,
                takerOrderID: takerID,
                price: 7000000000,
            },
        ]);
        assert_order_qtys<FMA, FMB>(marketAddr, takerID, 110000000000, 110000000000, 0);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert_order_used(book, takerID);
    }

    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="FOK order tests">

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    fun test_market_fok_order_cancelled_cache_limit_exceeded_middle_sell(aptos: &signer, ferum: &signer, user: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that an fok buy order that can't execute past the middle of the cache is cancelled.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 2);

        // Setup.
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 5000000000, 30000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 5000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 6000000000, 30000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 6000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 8000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 9000000000, 30000000000);
        let takerID = add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_FOK, 5500000000, 100000000000);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.9 qty: 3, crankQty: 0)"),
            s(b"(0.8 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
            s(b"(0.6 qty: 8, crankQty: 0)"),
            s(b"(0.5 qty: 7, crankQty: 0)"),
        ]);
        assert_exec_events<FMA, FMB>(marketAddr, vector[]);
        assert!(takerID == 0, 0);
    }

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    fun test_market_fok_order_cancelled_cache_limit_exceeded_front_sell(aptos: &signer, ferum: &signer, user: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that an fok buy order that can't execute past the front of the cache is cancelled.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 2);

        // Setup.
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 5000000000, 30000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 5000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 6000000000, 30000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 6000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 8000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 9000000000, 30000000000);
        let takerID = add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_FOK, 4000000000, 100000000000);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.9 qty: 3, crankQty: 0)"),
            s(b"(0.8 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
            s(b"(0.6 qty: 8, crankQty: 0)"),
            s(b"(0.5 qty: 7, crankQty: 0)"),
        ]);
        assert_exec_events<FMA, FMB>(marketAddr, vector[]);
        assert!(takerID == 0, 0);
    }

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    fun test_market_fok_order_cancelled_cache_empty_tree_not_enough_qty_sell(aptos: &signer, ferum: &signer, user: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that an fok buy order that can't execute in the tree because there is not enough qty is cancelled.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 2);

        // Setup.
        let orderID1 = add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 5000000000, 30000000000);
        let orderID2 = add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 6000000000, 30000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 8000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 9000000000, 30000000000);
        cancel_order<FMA, FMB>(user, orderID1);
        cancel_order<FMA, FMB>(user, orderID2);
        let takerID = add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_FOK, 9000000000, 1000000000000);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.9 qty: 3, crankQty: 0)"),
            s(b"(0.8 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[]);
        assert_exec_events<FMA, FMB>(marketAddr, vector[]);
        assert!(takerID == 0, 0);
    }

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    fun test_market_fok_order_cancelled_cache_empty_tree_limit_exceed_middle_sell(aptos: &signer, ferum: &signer, user: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that an fok buy order that can't execute in the tree because the limit price is exceeded is cancelled.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 2);

        // Setup.
        let orderID1 = add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 5000000000, 30000000000);
        let orderID2 = add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 6000000000, 30000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 8000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 9000000000, 30000000000);
        cancel_order<FMA, FMB>(user, orderID1);
        cancel_order<FMA, FMB>(user, orderID2);
        let takerID = add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_FOK, 8500000000, 100000000000);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.9 qty: 3, crankQty: 0)"),
            s(b"(0.8 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[]);
        assert_exec_events<FMA, FMB>(marketAddr, vector[]);
        assert!(takerID == 0, 0);
    }

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    fun test_market_fok_order_cancelled_empty_book_sell(aptos: &signer, ferum: &signer, user: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that an fok buy order that can't execute because the book is empty.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 2);

        // Setup.
        let takerID = add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_FOK, 8500000000, 100000000000);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[], vector[]);
        assert_exec_events<FMA, FMB>(marketAddr, vector[]);
        assert!(takerID == 0, 0);
    }

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    fun test_market_fok_order_cancelled_tree_limit_exceeded_front_sell(aptos: &signer, ferum: &signer, user: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that an fok buy order that can't execute past the front of the tree is cancelled.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 2);

        // Setup.
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 5000000000, 30000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 6000000000, 30000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 8000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 9000000000, 30000000000);
        let takerID = add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_FOK, 6500000000, 100000000000);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.9 qty: 3, crankQty: 0)"),
            s(b"(0.8 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
            s(b"(0.6 qty: 3, crankQty: 0)"),
            s(b"(0.5 qty: 3, crankQty: 0)"),
        ]);
        assert_exec_events<FMA, FMB>(marketAddr, vector[]);
        assert!(takerID == 0, 0);
    }

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    fun test_market_fok_order_cancelled_tree_limit_exceeded_middle_sell(aptos: &signer, ferum: &signer, user: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that an fok buy order that can't execute past middle of the tree is cancelled.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 2);

        // Setup.
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 5000000000, 30000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 6000000000, 30000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 8000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 9000000000, 30000000000);
        let takerID = add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_FOK, 7500000000, 150000000000);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.9 qty: 3, crankQty: 0)"),
            s(b"(0.8 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
            s(b"(0.6 qty: 3, crankQty: 0)"),
            s(b"(0.5 qty: 3, crankQty: 0)"),
        ]);
        assert_exec_events<FMA, FMB>(marketAddr, vector[]);
        assert!(takerID == 0, 0);
    }

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    fun test_market_fok_order_cancelled_tree_empty_sell(aptos: &signer, ferum: &signer, user: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that an fok buy order that is cancelled when there is not enough qty in the tree.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 2);

        // Setup.
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 5000000000, 30000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 6000000000, 30000000000);
        let takerID = add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_FOK, 7500000000, 150000000000);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[
        ], vector[
            s(b"(0.6 qty: 3, crankQty: 0)"),
            s(b"(0.5 qty: 3, crankQty: 0)"),
        ]);
        assert_exec_events<FMA, FMB>(marketAddr, vector[]);
        assert!(takerID == 0, 0);
    }

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    fun test_market_fok_order_executed_sell(aptos: &signer, ferum: &signer, user: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that an fok buy order is able to execute when there is enough qty.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 2);

        // Setup.
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 5000000000, 30000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 6000000000, 30000000000);
        let takerID = add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_FOK, 7500000000, 50000000000);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[
        ], vector[
            s(b"(0.6 qty: 1, crankQty: 2)"),
            s(b"(0.5 qty: 0, crankQty: 3)"),
        ]);
        assert_exec_events<FMA, FMB>(marketAddr, vector[
            ExecEventInfo {
                qty: 30000000000,
                takerOrderID: takerID,
                price: 5000000000,
            },
            ExecEventInfo {
                qty: 20000000000,
                takerOrderID: takerID,
                price: 6000000000,
            },
        ]);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert_order_used(book, takerID);
    }

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    fun test_market_fok_order_cancelled_cache_limit_exceeded_middle_buy(aptos: &signer, ferum: &signer, user: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that an fok sell order that can't execute past the middle of the cache is cancelled.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 2);

        // Setup.
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 9000000000, 30000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 8000000000, 30000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 6000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 5000000000, 30000000000);
        let takerID = add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_FOK, 8500000000, 100000000000);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.5 qty: 3, crankQty: 0)"),
            s(b"(0.6 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
            s(b"(0.8 qty: 3, crankQty: 0)"),
            s(b"(0.9 qty: 3, crankQty: 0)"),
        ]);
        assert_exec_events<FMA, FMB>(marketAddr, vector[]);
        assert!(takerID == 0, 0);
    }

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    fun test_market_fok_order_cancelled_cache_limit_exceeded_front_buy(aptos: &signer, ferum: &signer, user: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that an fok sell order that can't execute past the front of the cache is cancelled.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 2);

        // Setup.
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 9000000000, 30000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 8000000000, 30000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 6000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 5000000000, 30000000000);
        let takerID = add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_FOK, 9500000000, 100000000000);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.5 qty: 3, crankQty: 0)"),
            s(b"(0.6 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
            s(b"(0.8 qty: 3, crankQty: 0)"),
            s(b"(0.9 qty: 3, crankQty: 0)"),
        ]);
        assert_exec_events<FMA, FMB>(marketAddr, vector[]);
        assert!(takerID == 0, 0);
    }

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    fun test_market_fok_order_cancelled_cache_empty_tree_not_enough_qty_buy(aptos: &signer, ferum: &signer, user: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that an fok sell order that can't execute in the tree because there is not enough qty is cancelled.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 2);

        // Setup.
        let orderID1 = add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 9000000000, 30000000000);
        let orderID2 = add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 8000000000, 30000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 6000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 5000000000, 30000000000);
        cancel_order<FMA, FMB>(user, orderID1);
        cancel_order<FMA, FMB>(user, orderID2);
        let takerID = add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_FOK, 4500000000, 1000000000000);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.5 qty: 3, crankQty: 0)"),
            s(b"(0.6 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
        ]);
        assert_exec_events<FMA, FMB>(marketAddr, vector[]);
        assert!(takerID == 0, 0);
    }

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    fun test_market_fok_order_cancelled_cache_empty_tree_limit_exceed_middle_buy(aptos: &signer, ferum: &signer, user: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that an fok sell order that can't execute in the tree because the limit price is exceeded is cancelled.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 2);

        // Setup.
        let orderID1 = add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 9000000000, 30000000000);
        let orderID2 = add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 8000000000, 30000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 6000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 5000000000, 30000000000);
        cancel_order<FMA, FMB>(user, orderID1);
        cancel_order<FMA, FMB>(user, orderID2);
        let takerID = add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_FOK, 6500000000, 60000000000);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.5 qty: 3, crankQty: 0)"),
            s(b"(0.6 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
        ]);
        assert_exec_events<FMA, FMB>(marketAddr, vector[]);
        assert!(takerID == 0, 0);
    }

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    fun test_market_fok_order_cancelled_empty_book_buy(aptos: &signer, ferum: &signer, user: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that an fok sell order that can't execute because the book is empty.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 2);

        // Setup.
        let takerID = add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_FOK, 8500000000, 100000000000);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[], vector[]);
        assert_exec_events<FMA, FMB>(marketAddr, vector[]);
        assert!(takerID == 0, 0);
    }

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    fun test_market_fok_order_cancelled_tree_limit_exceeded_front_buy(aptos: &signer, ferum: &signer, user: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that an fok sell order that can't execute past the front of the tree is cancelled.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 2);

        // Setup.
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 9000000000, 30000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 8000000000, 30000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 6000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 5000000000, 30000000000);
        let takerID = add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_FOK, 7500000000, 100000000000);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.5 qty: 3, crankQty: 0)"),
            s(b"(0.6 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
            s(b"(0.8 qty: 3, crankQty: 0)"),
            s(b"(0.9 qty: 3, crankQty: 0)"),
        ]);
        assert_exec_events<FMA, FMB>(marketAddr, vector[]);
        assert!(takerID == 0, 0);
    }

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    fun test_market_fok_order_cancelled_tree_limit_exceeded_middle_buy(aptos: &signer, ferum: &signer, user: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that an fok sell order that can't execute past middle of the tree is cancelled.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 2);

        // Setup.
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 9000000000, 30000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 8000000000, 30000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 6000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 5000000000, 30000000000);
        let takerID = add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_FOK, 6500000000, 150000000000);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.5 qty: 3, crankQty: 0)"),
            s(b"(0.6 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
            s(b"(0.8 qty: 3, crankQty: 0)"),
            s(b"(0.9 qty: 3, crankQty: 0)"),
        ]);
        assert_exec_events<FMA, FMB>(marketAddr, vector[]);
        assert!(takerID == 0, 0);
    }

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    fun test_market_fok_order_cancelled_tree_empty_buy(aptos: &signer, ferum: &signer, user: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that an fok sell order that is cancelled when there is not enough qty in the tree.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 2);

        // Setup.
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 9000000000, 30000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 8000000000, 30000000000);
        let takerID = add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_FOK, 7500000000, 100000000000);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
        ], vector[
            s(b"(0.8 qty: 3, crankQty: 0)"),
            s(b"(0.9 qty: 3, crankQty: 0)"),
        ]);
        assert_exec_events<FMA, FMB>(marketAddr, vector[]);
        assert!(takerID == 0, 0);
    }

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    fun test_market_fok_order_executed_buy(aptos: &signer, ferum: &signer, user: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that an fok sell order is able to execute when there is enough qty.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 2);

        // Setup.
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 6000000000, 30000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 5000000000, 30000000000);
        let takerID = add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_FOK, 4500000000, 50000000000);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
        ], vector[
            s(b"(0.5 qty: 1, crankQty: 2)"),
            s(b"(0.6 qty: 0, crankQty: 3)"),
        ]);
        assert_exec_events<FMA, FMB>(marketAddr, vector[
            ExecEventInfo {
                qty: 30000000000,
                takerOrderID: takerID,
                price: 6000000000,
            },
            ExecEventInfo {
                qty: 20000000000,
                takerOrderID: takerID,
                price: 5000000000,
            },
        ]);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert_order_used(book, takerID);
    }

    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="Rebalance">

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    fun test_market_rebalance(
        aptos: &signer,
        ferum: &signer,
        user: &signer,
    )
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 2);

        // Setup sell side.
        let orderID = add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 5000000000, 30000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 6000000000, 30000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 6000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 8000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 9000000000, 30000000000);
        cancel_order<FMA, FMB>(user, orderID);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.9 qty: 3, crankQty: 0)"),
            s(b"(0.8 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
            s(b"(0.6 qty: 8, crankQty: 0)"),
        ]);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert!(book.summary.sellCacheMax == 6000000000, 0);
        assert!(book.summary.sellCacheMin == 6000000000, 0);
        assert!(book.summary.sellCacheQty == 80000000000, 0);
        assert!(book.summary.sellCacheSize == 1, 0);
        assert!(book.summary.sellTreeMin == 7000000000, 0);
        // Setup buy side.
        let orderID = add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 5500000000, 30000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 5000000000, 30000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 5000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 4000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 3000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 2000000000, 30000000000);
        cancel_order<FMA, FMB>(user, orderID);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.2 qty: 3, crankQty: 0)"),
            s(b"(0.3 qty: 4, crankQty: 0)"),
            s(b"(0.4 qty: 5, crankQty: 0)"),
        ], vector[
            s(b"(0.5 qty: 8, crankQty: 0)"),
        ]);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert!(book.summary.buyCacheMax == 5000000000, 0);
        assert!(book.summary.buyCacheMin == 5000000000, 0);
        assert!(book.summary.buyCacheQty == 80000000000, 0);
        assert!(book.summary.buyCacheSize == 1, 0);
        assert!(book.summary.buyTreeMax == 4000000000, 0);

        rebalance_cache_entry<FMA, FMB>(user, 1);

        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.9 qty: 3, crankQty: 0)"),
            s(b"(0.8 qty: 4, crankQty: 0)"),
        ], vector[
            s(b"(0.7 qty: 5, crankQty: 0)"),
            s(b"(0.6 qty: 8, crankQty: 0)"),
        ]);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.2 qty: 3, crankQty: 0)"),
            s(b"(0.3 qty: 4, crankQty: 0)"),
        ], vector[
            s(b"(0.4 qty: 5, crankQty: 0)"),
            s(b"(0.5 qty: 8, crankQty: 0)"),
        ]);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert!(book.summary.buyCacheMax == 5000000000, 0);
        assert!(book.summary.buyCacheMin == 4000000000, 0);
        assert!(book.summary.buyCacheQty == 130000000000, 0);
        assert!(book.summary.buyCacheSize == 2, 0);
        assert!(book.summary.buyTreeMax == 3000000000, 0);
        assert!(book.summary.sellCacheMax == 7000000000, 0);
        assert!(book.summary.sellCacheMin == 6000000000, 0);
        assert!(book.summary.sellCacheQty == 130000000000, 0);
        assert!(book.summary.sellCacheSize == 2, 0);
        assert!(book.summary.sellTreeMin == 8000000000, 0);
    }

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    fun test_market_rebalance_empty_tree(
        aptos: &signer,
        ferum: &signer,
        user: &signer,
    )
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 2);

        // Setup sell side.
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 6000000000, 30000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 6000000000, 50000000000);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[
        ], vector[
            s(b"(0.6 qty: 8, crankQty: 0)"),
        ]);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert!(book.summary.sellCacheMax == 6000000000, 0);
        assert!(book.summary.sellCacheMin == 6000000000, 0);
        assert!(book.summary.sellCacheQty == 80000000000, 0);
        assert!(book.summary.sellCacheSize == 1, 0);
        assert!(book.summary.sellTreeMin == 0, 0);
        // Setup buy side.
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 5000000000, 30000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 5000000000, 50000000000);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
        ], vector[
            s(b"(0.5 qty: 8, crankQty: 0)"),
        ]);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert!(book.summary.buyCacheMax == 5000000000, 0);
        assert!(book.summary.buyCacheMin == 5000000000, 0);
        assert!(book.summary.buyCacheQty == 80000000000, 0);
        assert!(book.summary.buyCacheSize == 1, 0);
        assert!(book.summary.buyTreeMax == 0, 0);

        rebalance_cache_entry<FMA, FMB>(user, 1);

        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[
        ], vector[
            s(b"(0.6 qty: 8, crankQty: 0)"),
        ]);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
        ], vector[
            s(b"(0.5 qty: 8, crankQty: 0)"),
        ]);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert!(book.summary.buyCacheMax == 5000000000, 0);
        assert!(book.summary.buyCacheMin == 5000000000, 0);
        assert!(book.summary.buyCacheQty == 80000000000, 0);
        assert!(book.summary.buyCacheSize == 1, 0);
        assert!(book.summary.buyTreeMax == 0, 0);
        assert!(book.summary.sellCacheMax == 6000000000, 0);
        assert!(book.summary.sellCacheMin == 6000000000, 0);
        assert!(book.summary.sellCacheQty == 80000000000, 0);
        assert!(book.summary.sellCacheSize == 1, 0);
        assert!(book.summary.sellTreeMin == 0, 0);
    }

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    fun test_market_rebalance_full_cache(
        aptos: &signer,
        ferum: &signer,
        user: &signer,
    )
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 2);

        // Setup sell side.
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 5600000000, 30000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 6000000000, 30000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 6000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 8000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 9000000000, 30000000000);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.9 qty: 3, crankQty: 0)"),
            s(b"(0.8 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
            s(b"(0.6 qty: 8, crankQty: 0)"),
            s(b"(0.56 qty: 3, crankQty: 0)"),
        ]);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert!(book.summary.sellCacheMax == 6000000000, 0);
        assert!(book.summary.sellCacheMin == 5600000000, 0);
        assert!(book.summary.sellCacheQty == 110000000000, 0);
        assert!(book.summary.sellCacheSize == 2, 0);
        assert!(book.summary.sellTreeMin == 7000000000, 0);
        // Setup buy side.
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 5500000000, 30000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 5000000000, 30000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 5000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 4000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 3000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 2000000000, 30000000000);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.2 qty: 3, crankQty: 0)"),
            s(b"(0.3 qty: 4, crankQty: 0)"),
            s(b"(0.4 qty: 5, crankQty: 0)"),
        ], vector[
            s(b"(0.5 qty: 8, crankQty: 0)"),
            s(b"(0.55 qty: 3, crankQty: 0)"),
        ]);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert!(book.summary.buyCacheMax == 5500000000, 0);
        assert!(book.summary.buyCacheMin == 5000000000, 0);
        assert!(book.summary.buyCacheQty == 110000000000, 0);
        assert!(book.summary.buyCacheSize == 2, 0);
        assert!(book.summary.buyTreeMax == 4000000000, 0);

        rebalance_cache_entry<FMA, FMB>(user, 1);

        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.9 qty: 3, crankQty: 0)"),
            s(b"(0.8 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
            s(b"(0.6 qty: 8, crankQty: 0)"),
            s(b"(0.56 qty: 3, crankQty: 0)"),
        ]);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.2 qty: 3, crankQty: 0)"),
            s(b"(0.3 qty: 4, crankQty: 0)"),
            s(b"(0.4 qty: 5, crankQty: 0)"),
        ], vector[
            s(b"(0.5 qty: 8, crankQty: 0)"),
            s(b"(0.55 qty: 3, crankQty: 0)"),
        ]);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert!(book.summary.buyCacheMax == 5500000000, 0);
        assert!(book.summary.buyCacheMin == 5000000000, 0);
        assert!(book.summary.buyCacheQty == 110000000000, 0);
        assert!(book.summary.buyCacheSize == 2, 0);
        assert!(book.summary.buyTreeMax == 4000000000, 0);
        assert!(book.summary.sellCacheMax == 6000000000, 0);
        assert!(book.summary.sellCacheMin == 5600000000, 0);
        assert!(book.summary.sellCacheQty == 110000000000, 0);
        assert!(book.summary.sellCacheSize == 2, 0);
        assert!(book.summary.sellTreeMin == 7000000000, 0);
    }

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    fun test_market_rebalance_multiple_items(
        aptos: &signer,
        ferum: &signer,
        user: &signer,
    )
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 4);

        // Setup sell side.
        let orderID1 = add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 5600000000, 30000000000);
        let orderID2 = add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 6000000000, 30000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 8000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 9000000000, 30000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 10000000000, 30000000000);
        cancel_order<FMA, FMB>(user, orderID1);
        cancel_order<FMA, FMB>(user, orderID2);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(1 qty: 3, crankQty: 0)"),
            s(b"(0.9 qty: 3, crankQty: 0)"),
        ], vector[
            s(b"(0.8 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ]);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert!(book.summary.sellCacheMax == 8000000000, 0);
        assert!(book.summary.sellCacheMin == 7000000000, 0);
        assert!(book.summary.sellCacheQty == 90000000000, 0);
        assert!(book.summary.sellCacheSize == 2, 0);
        assert!(book.summary.sellTreeMin == 9000000000, 0);
        // Setup buy side.
        let orderID1 = add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 5500000000, 30000000000);
        let orderID2 = add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 5000000000, 30000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 4000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 3000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 2000000000, 30000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 1000000000, 30000000000);
        cancel_order<FMA, FMB>(user, orderID1);
        cancel_order<FMA, FMB>(user, orderID2);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.1 qty: 3, crankQty: 0)"),
            s(b"(0.2 qty: 3, crankQty: 0)"),
        ], vector[
            s(b"(0.3 qty: 4, crankQty: 0)"),
            s(b"(0.4 qty: 5, crankQty: 0)"),
        ]);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert!(book.summary.buyCacheMax == 4000000000, 0);
        assert!(book.summary.buyCacheMin == 3000000000, 0);
        assert!(book.summary.buyCacheQty == 90000000000, 0);
        assert!(book.summary.buyCacheSize == 2, 0);
        assert!(book.summary.buyTreeMax == 2000000000, 0);

        rebalance_cache_entry<FMA, FMB>(user, 2);

        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[
        ], vector[
            s(b"(1 qty: 3, crankQty: 0)"),
            s(b"(0.9 qty: 3, crankQty: 0)"),
            s(b"(0.8 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ]);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
        ], vector[
            s(b"(0.1 qty: 3, crankQty: 0)"),
            s(b"(0.2 qty: 3, crankQty: 0)"),
            s(b"(0.3 qty: 4, crankQty: 0)"),
            s(b"(0.4 qty: 5, crankQty: 0)"),
        ]);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert!(book.summary.buyCacheMax == 4000000000, 0);
        assert!(book.summary.buyCacheMin == 1000000000, 0);
        assert!(book.summary.buyCacheQty == 150000000000, 0);
        assert!(book.summary.buyCacheSize == 4, 0);
        assert!(book.summary.buyTreeMax == 0, 0);
        assert!(book.summary.sellCacheMax == 10000000000, 0);
        assert!(book.summary.sellCacheMin == 7000000000, 0);
        assert!(book.summary.sellCacheQty == 150000000000, 0);
        assert!(book.summary.sellCacheSize == 4, 0);
        assert!(book.summary.sellTreeMin == 0, 0);
    }

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    fun test_market_rebalance_limit(
        aptos: &signer,
        ferum: &signer,
        user: &signer,
    )
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 4);

        // Setup sell side.
        let orderID1 = add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 5600000000, 30000000000);
        let orderID2 = add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 6000000000, 30000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 8000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 9000000000, 30000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 10000000000, 30000000000);
        cancel_order<FMA, FMB>(user, orderID1);
        cancel_order<FMA, FMB>(user, orderID2);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(1 qty: 3, crankQty: 0)"),
            s(b"(0.9 qty: 3, crankQty: 0)"),
        ], vector[
            s(b"(0.8 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ]);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert!(book.summary.sellCacheMax == 8000000000, 0);
        assert!(book.summary.sellCacheMin == 7000000000, 0);
        assert!(book.summary.sellCacheQty == 90000000000, 0);
        assert!(book.summary.sellCacheSize == 2, 0);
        assert!(book.summary.sellTreeMin == 9000000000, 0);
        // Setup buy side.
        let orderID1 = add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 5500000000, 30000000000);
        let orderID2 = add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 5000000000, 30000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 4000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 3000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 2000000000, 30000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 1000000000, 30000000000);
        cancel_order<FMA, FMB>(user, orderID1);
        cancel_order<FMA, FMB>(user, orderID2);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.1 qty: 3, crankQty: 0)"),
            s(b"(0.2 qty: 3, crankQty: 0)"),
        ], vector[
            s(b"(0.3 qty: 4, crankQty: 0)"),
            s(b"(0.4 qty: 5, crankQty: 0)"),
        ]);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert!(book.summary.buyCacheMax == 4000000000, 0);
        assert!(book.summary.buyCacheMin == 3000000000, 0);
        assert!(book.summary.buyCacheQty == 90000000000, 0);
        assert!(book.summary.buyCacheSize == 2, 0);
        assert!(book.summary.buyTreeMax == 2000000000, 0);

        rebalance_cache_entry<FMA, FMB>(user, 1);

        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(1 qty: 3, crankQty: 0)"),
        ], vector[
            s(b"(0.9 qty: 3, crankQty: 0)"),
            s(b"(0.8 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ]);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.1 qty: 3, crankQty: 0)"),
        ], vector[
            s(b"(0.2 qty: 3, crankQty: 0)"),
            s(b"(0.3 qty: 4, crankQty: 0)"),
            s(b"(0.4 qty: 5, crankQty: 0)"),
        ]);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert!(book.summary.buyCacheMax == 4000000000, 0);
        assert!(book.summary.buyCacheMin == 2000000000, 0);
        assert!(book.summary.buyCacheQty == 120000000000, 0);
        assert!(book.summary.buyCacheSize == 3, 0);
        assert!(book.summary.buyTreeMax == 1000000000, 0);
        assert!(book.summary.sellCacheMax == 9000000000, 0);
        assert!(book.summary.sellCacheMin == 7000000000, 0);
        assert!(book.summary.sellCacheQty == 120000000000, 0);
        assert!(book.summary.sellCacheSize == 3, 0);
        assert!(book.summary.sellTreeMin == 10000000000, 0);
    }

    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="Market order tests">

    #[test(aptos=@0x1, ferum=@ferum, user1=@0x3, makerUser=@0x4, takerUser=@0x6)]
    fun test_market_market_buy_exec_run_out_of_collateral_cache(
        aptos: &signer,
        ferum: &signer,
        user1: &signer,
        makerUser: &signer,
        takerUser: &signer,
    )
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that market buy order executes against orders in the cache properly.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user1, 2);
        account::create_account_for_test(address_of(makerUser));
        deposit_fake_coins(ferum, 10000000000, makerUser);
        account::create_account_for_test(address_of(takerUser));
        deposit_fake_coins(ferum, 10000000000, takerUser);
        let makerAccID = platform::account_identifier_for_test(makerUser);
        let makerAccKey = open_market_account<FMA, FMB>(makerUser, vector[makerAccID]);
        deposit_to_market_account<FMA, FMB>(makerUser, makerAccKey, 1000000000000, 1000000000000);
        let takerAccID = platform::account_identifier_for_test(takerUser);
        let takerAccKey = open_market_account<FMA, FMB>(takerUser, vector[takerAccID]);
        deposit_to_market_account<FMA, FMB>(takerUser, takerAccKey, 1000000000000, 1000000000000);

        // Setup.
        let makerID1 = add_user_limit_order<FMA, FMB>(makerUser, SIDE_SELL, BEHAVIOUR_GTC, 5000000000, 30000000000);
        let makerID2 = add_user_limit_order<FMA, FMB>(makerUser, SIDE_SELL, BEHAVIOUR_GTC, 5000000000, 40000000000);
        let makerID3 = add_user_limit_order<FMA, FMB>(makerUser, SIDE_SELL, BEHAVIOUR_GTC, 6000000000, 30000000000);
        let makerID4 = add_user_limit_order<FMA, FMB>(makerUser, SIDE_SELL, BEHAVIOUR_GTC, 6000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user1, SIDE_SELL, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user1, SIDE_SELL, BEHAVIOUR_GTC, 8000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user1, SIDE_SELL, BEHAVIOUR_GTC, 9000000000, 30000000000);
        let takerID = add_user_market_order<FMA, FMB>(takerUser, SIDE_BUY, BEHAVIOUR_IOC, 270000000000, 64000000000);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.9 qty: 3, crankQty: 0)"),
            s(b"(0.8 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
            s(b"(0.6 qty: 3.1667, crankQty: 4.8333)"),
            s(b"(0.5 qty: 0, crankQty: 7)"),
        ]);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[], vector[]);
        assert_exec_events<FMA, FMB>(marketAddr, vector[
            ExecEventInfo {
                qty: 70000000000,
                takerOrderID: takerID,
                price: 5000000000,
            },
            ExecEventInfo {
                qty: 48333000000,
                takerOrderID: takerID,
                price: 6000000000,
            },
        ]);
        assert_order_qtys<FMA, FMB>(marketAddr, makerID1, 30000000000, 0, 30000000000);
        assert_order_qtys<FMA, FMB>(marketAddr, makerID2, 40000000000, 0, 40000000000);
        assert_order_qtys<FMA, FMB>(marketAddr, makerID3, 30000000000, 0, 30000000000);
        assert_order_qtys<FMA, FMB>(marketAddr, makerID4, 50000000000, 0, 18333000000);
        assert_order_qtys<FMA, FMB>(marketAddr, takerID, 270000000000, 118333000000, 0);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert_order_collateral(book, takerID, 64000000000, 0);
        assert_order_collateral(book, makerID1, 0, 30000000000);
        assert_order_collateral(book, makerID2, 0, 40000000000);
        assert_order_collateral(book, makerID3, 0, 30000000000);
        assert_order_collateral(book, makerID4, 0, 50000000000);
        assert_order_used(book, takerID);
        assert_order_used(book, makerID1);
        assert_order_used(book, makerID2);
        assert_order_used(book, makerID3);
        assert_order_used(book, makerID4);
        assert_account_balances(book, takerAccKey, 1000000000000, 936000000000);
        assert_account_balances(book, makerAccKey, 850000000000, 1000000000000);
        assert!(book.summary.sellCacheMax == 6000000000, 0);
        assert!(book.summary.sellCacheMin == 5000000000, 0);
        assert!(book.summary.sellCacheQty == 31667000000, 0);
        assert!(book.summary.sellCacheSize == 2, 0);
        assert!(book.summary.sellTreeMin == 7000000000, 0);

        // Crank.
        crank<FMA, FMB>(user1, 1);
        assert_exec_events<FMA, FMB>(marketAddr, vector[]);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.9 qty: 3, crankQty: 0)"),
            s(b"(0.8 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
            s(b"(0.6 qty: 3.1667, crankQty: 0)"),
        ]);
        assert_order_qtys<FMA, FMB>(marketAddr, takerID, 0, 0, 0); // No longer used.
        assert_order_qtys<FMA, FMB>(marketAddr, makerID1, 0, 0, 0); // No longer used.
        assert_order_qtys<FMA, FMB>(marketAddr, makerID2, 0, 0, 0); // No longer used.
        assert_order_qtys<FMA, FMB>(marketAddr, makerID3, 0, 0, 0); // No longer used.
        assert_order_qtys<FMA, FMB>(marketAddr, makerID4, 31667000000, 0, 0);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert_order_collateral(book, makerID1, 0, 0);
        assert_order_collateral(book, makerID2, 0, 0);
        assert_order_collateral(book, makerID3, 0, 0);
        assert_order_collateral(book, makerID4, 0, 31667000000);
        assert_order_collateral(book, takerID, 0, 0);
        assert_order_unused(book, takerID);
        assert_order_unused(book, makerID1);
        assert_order_unused(book, makerID2);
        assert_order_unused(book, makerID3);
        assert_order_used(book, makerID4);
        assert_account_balances(book, makerAccKey, 850000000000, 1063999800000);
        assert_account_balances(book, takerAccKey, 1118333000000, 936000200000);
        assert!(book.summary.sellCacheMax == 6000000000, 0);
        assert!(book.summary.sellCacheMin == 6000000000, 0);
        assert!(book.summary.sellCacheQty == 31667000000, 0);
        assert!(book.summary.sellCacheSize == 1, 0);
        assert!(book.summary.sellTreeMin == 7000000000, 0);
    }

    #[test(aptos=@0x1, ferum=@ferum, user1=@0x3, makerUser=@0x4, takerUser=@0x6)]
    fun test_market_market_buy_exec_run_out_of_collateral_tree(
        aptos: &signer,
        ferum: &signer,
        user1: &signer,
        makerUser: &signer,
        takerUser: &signer,
    )
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that market buy order executes against orders in the tree properly.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user1, 2);
        account::create_account_for_test(address_of(makerUser));
        deposit_fake_coins(ferum, 10000000000, makerUser);
        account::create_account_for_test(address_of(takerUser));
        deposit_fake_coins(ferum, 10000000000, takerUser);
        let makerAccID = platform::account_identifier_for_test(makerUser);
        let makerAccKey = open_market_account<FMA, FMB>(makerUser, vector[makerAccID]);
        deposit_to_market_account<FMA, FMB>(makerUser, makerAccKey, 1000000000000, 1000000000000);
        let takerAccID = platform::account_identifier_for_test(takerUser);
        let takerAccKey = open_market_account<FMA, FMB>(takerUser, vector[takerAccID]);
        deposit_to_market_account<FMA, FMB>(takerUser, takerAccKey, 1000000000000, 1000000000000);

        // Setup.
        let makerID1 = add_user_limit_order<FMA, FMB>(makerUser, SIDE_SELL, BEHAVIOUR_GTC, 5000000000, 30000000000);
        let makerID2 = add_user_limit_order<FMA, FMB>(makerUser, SIDE_SELL, BEHAVIOUR_GTC, 5000000000, 40000000000);
        let makerID3 = add_user_limit_order<FMA, FMB>(makerUser, SIDE_SELL, BEHAVIOUR_GTC, 6000000000, 30000000000);
        let makerID4 = add_user_limit_order<FMA, FMB>(makerUser, SIDE_SELL, BEHAVIOUR_GTC, 6000000000, 50000000000);
        let makerID5 = add_user_limit_order<FMA, FMB>(makerUser, SIDE_SELL, BEHAVIOUR_GTC, 7000000000, 10000000000);
        let makerID6 = add_user_limit_order<FMA, FMB>(makerUser, SIDE_SELL, BEHAVIOUR_GTC, 7000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user1, SIDE_SELL, BEHAVIOUR_GTC, 8000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user1, SIDE_SELL, BEHAVIOUR_GTC, 9000000000, 30000000000);
        let takerID = add_user_market_order<FMA, FMB>(takerUser, SIDE_BUY, BEHAVIOUR_IOC, 270000000000, 92614000000);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.9 qty: 3, crankQty: 0)"),
            s(b"(0.8 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 3.6266, crankQty: 1.3734)"),
        ], vector[
            s(b"(0.6 qty: 0, crankQty: 8)"),
            s(b"(0.5 qty: 0, crankQty: 7)"),
        ]);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[], vector[]);
        assert_exec_events<FMA, FMB>(marketAddr, vector[
            ExecEventInfo {
                qty: 70000000000,
                takerOrderID: takerID,
                price: 5000000000,
            },
            ExecEventInfo {
                qty: 80000000000,
                takerOrderID: takerID,
                price: 6000000000,
            },
            ExecEventInfo {
                qty: 13734000000,
                takerOrderID: takerID,
                price: 7000000000,
            },
        ]);
        assert_order_qtys<FMA, FMB>(marketAddr, makerID1, 30000000000, 0, 30000000000);
        assert_order_qtys<FMA, FMB>(marketAddr, makerID2, 40000000000, 0, 40000000000);
        assert_order_qtys<FMA, FMB>(marketAddr, makerID3, 30000000000, 0, 30000000000);
        assert_order_qtys<FMA, FMB>(marketAddr, makerID4, 50000000000, 0, 50000000000);
        assert_order_qtys<FMA, FMB>(marketAddr, makerID5, 10000000000, 0, 10000000000);
        assert_order_qtys<FMA, FMB>(marketAddr, makerID6, 40000000000, 0, 03734000000);
        assert_order_qtys<FMA, FMB>(marketAddr, takerID, 270000000000, 163734000000, 0);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert_order_collateral(book, takerID, 92614000000, 0);
        assert_order_collateral(book, makerID1, 0, 30000000000);
        assert_order_collateral(book, makerID2, 0, 40000000000);
        assert_order_collateral(book, makerID3, 0, 30000000000);
        assert_order_collateral(book, makerID4, 0, 50000000000);
        assert_order_collateral(book, makerID5, 0, 10000000000);
        assert_order_collateral(book, makerID6, 0, 40000000000);
        assert_order_used(book, takerID);
        assert_order_used(book, makerID1);
        assert_order_used(book, makerID2);
        assert_order_used(book, makerID3);
        assert_order_used(book, makerID4);
        assert_order_used(book, makerID5);
        assert_order_used(book, makerID6);
        assert_account_balances(book, takerAccKey, 1000000000000, 907386000000);
        assert_account_balances(book, makerAccKey, 800000000000, 1000000000000);
        assert!(book.summary.sellCacheMax == 6000000000, 0);
        assert!(book.summary.sellCacheMin == 5000000000, 0);
        assert!(book.summary.sellCacheQty == 0, 0);
        assert!(book.summary.sellCacheSize == 2, 0);
        assert!(book.summary.sellTreeMin == 7000000000, 0);

        // Crank.
        crank<FMA, FMB>(user1, 1);
        assert_exec_events<FMA, FMB>(marketAddr, vector[]);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.9 qty: 3, crankQty: 0)"),
            s(b"(0.8 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 3.6266, crankQty: 0)"),
        ], vector[]);
        assert_order_qtys<FMA, FMB>(marketAddr, takerID, 0, 0, 0); // No longer used.
        assert_order_qtys<FMA, FMB>(marketAddr, makerID1, 0, 0, 0); // No longer used.
        assert_order_qtys<FMA, FMB>(marketAddr, makerID2, 0, 0, 0); // No longer used.
        assert_order_qtys<FMA, FMB>(marketAddr, makerID3, 0, 0, 0); // No longer used.
        assert_order_qtys<FMA, FMB>(marketAddr, makerID4, 0, 0, 0); // No longer used.
        assert_order_qtys<FMA, FMB>(marketAddr, makerID5, 0, 0, 0); // No longer used.
        assert_order_qtys<FMA, FMB>(marketAddr, makerID6, 36266000000, 0, 0);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert_order_collateral(book, makerID1, 0, 0);
        assert_order_collateral(book, makerID2, 0, 0);
        assert_order_collateral(book, makerID3, 0, 0);
        assert_order_collateral(book, makerID4, 0, 0);
        assert_order_collateral(book, makerID5, 0, 0);
        assert_order_collateral(book, makerID6, 0, 36266000000);
        assert_order_collateral(book, takerID, 0, 0);
        assert_order_unused(book, takerID);
        assert_order_unused(book, makerID1);
        assert_order_unused(book, makerID2);
        assert_order_unused(book, makerID3);
        assert_order_unused(book, makerID4);
        assert_order_unused(book, makerID5);
        assert_order_used(book, makerID6);
        assert_account_balances(book, makerAccKey, 800000000000, 1092613800000);
        assert_account_balances(book, takerAccKey, 1163734000000, 907386200000);
        assert!(book.summary.sellCacheMax == 0, 0);
        assert!(book.summary.sellCacheMin == 0, 0);
        assert!(book.summary.sellCacheQty == 0, 0);
        assert!(book.summary.sellCacheSize == 0, 0);
        assert!(book.summary.sellTreeMin == 7000000000, 0);
    }

    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="Unit tests">

    #[test]
    fun test_unit_price_levels_prealloc() {
        let priceLevels = PriceLevelReuseTable{
            objects: table::new(),
            unusedStack: 0,
            currID: 1,
        };
        prealloc_price_levels(&mut priceLevels, 3);
        assert!(priceLevels.currID == 4, 0);
        let id = get_or_create_price_level(&mut priceLevels);
        assert!(table::contains(&priceLevels.objects, id), 0);
        assert!(priceLevels.currID == 4, 0);
        let id = get_or_create_price_level(&mut priceLevels);
        assert!(table::contains(&priceLevels.objects, id), 0);
        assert!(priceLevels.currID == 4, 0);
        let id = get_or_create_price_level(&mut priceLevels);
        assert!(table::contains(&priceLevels.objects, id), 0);
        assert!(priceLevels.currID == 4, 0);
        let id = get_or_create_price_level(&mut priceLevels);
        assert!(table::contains(&priceLevels.objects, id), 0);
        assert!(priceLevels.currID == 5, 0);

        drop_price_level_table(priceLevels);
    }

    #[test]
    fun test_unit_orders_prealloc() {
        let orders = OrderReuseTable<FMA, FMB>{
            objects: table::new(),
            unusedStack: 0,
            currID: 1,
        };
        prealloc_orders(&mut orders, 3);
        assert!(orders.currID == 4, 0);
        let id = get_or_create_order(&mut orders);
        assert!(table::contains(&orders.objects, id), 0);
        assert!(orders.currID == 4, 0);
        let id = get_or_create_order(&mut orders);
        assert!(table::contains(&orders.objects, id), 0);
        assert!(orders.currID == 4, 0);
        let id = get_or_create_order(&mut orders);
        assert!(table::contains(&orders.objects, id), 0);
        assert!(orders.currID == 4, 0);
        let id = get_or_create_order(&mut orders);
        assert!(table::contains(&orders.objects, id), 0);
        assert!(orders.currID == 5, 0);
        let OrderReuseTable {
            objects,
            unusedStack: _,
            currID: _,
        } = orders;
        table::drop_unchecked(objects);
    }

    #[test]
    fun test_unit_add_price_qty_to_cache() {
        let priceLevels = PriceLevelReuseTable{
            objects: table::new(),
            unusedStack: 0,
            currID: 1,
        };
        let cache = new_cache(SIDE_BUY);
        cache_insert(&mut cache, 1, PriceStoreElem{
            qty: 10,
            makerCrankPendingQty: 0,
            priceLevelID: 3,
        });
        // Existing price level in cache.
        let id = add_price_qty_to_cache(&mut cache, &mut priceLevels, 1, 10);
        assert!(id == 3, 0);
        let idx = *vector::borrow(&cache_find(&cache, 1), 0);
        let elem = vector::borrow(&cache.list, idx);
        assert!(elem.value.qty == 20, 0);
        assert!(elem.value.priceLevelID == 3, 0);
        assert!(priceLevels.currID == 1, 0);
        // New price level.
        let id = add_price_qty_to_cache(&mut cache, &mut priceLevels, 9, 10);
        assert!(id == 1, 0);
        let idx = *vector::borrow(&cache_find(&cache, 9), 0);
        let elem = vector::borrow(&cache.list,idx);
        assert!(elem.value.qty == 10, 0);
        assert!(elem.value.priceLevelID == 1, 0);
        assert!(priceLevels.currID == 2, 0);

        drop_cache(cache);
        drop_price_level_table(priceLevels);
    }

    #[test]
    fun test_unit_add_price_qty_to_tree() {
        let priceLevels = PriceLevelReuseTable{
            objects: table::new(),
            unusedStack: 0,
            currID: 1,
        };
        let tree = new_tree<PriceStoreElem>(8);
        tree_insert(&mut tree, 1, PriceStoreElem{
            qty: 10,
            makerCrankPendingQty: 0,
            priceLevelID: 3,
        });
        // Existing price level in tree.
        let id = add_price_qty_to_tree(&mut tree, &mut priceLevels, 1, 10);
        assert!(id == 3, 0);
        let pos = &tree_find(&tree, 1);
        assert!(pos.nodeID != 0, 0);
        let (price, elem) = tree_get_mut(&mut tree, pos);
        assert!(price == 1, 0);
        assert!(elem.qty == 20, 0);
        assert!(elem.priceLevelID == 3, 0);
        assert!(priceLevels.currID == 1, 0);
        // New price level.
        let id = add_price_qty_to_tree(&mut tree, &mut priceLevels, 9, 10);
        assert!(id == 1, 0);
        let pos = &tree_find(&tree, 9);
        assert!(pos.nodeID != 0, 0);
        let (price, elem) = tree_get_mut(&mut tree, pos);
        assert!(price == 9, 0);
        assert!(elem.qty == 10, 0);
        assert!(elem.priceLevelID == 1, 0);
        assert!(priceLevels.currID == 2, 0);

        destroy_tree(tree);
        drop_price_level_table(priceLevels);
    }

    #[test]
    fun test_unit_remove_price_qty_from_tree_non_crank_no_pos_hint() {
        let tree = new_tree<PriceStoreElem>(8);
        tree_insert(&mut tree, 1, PriceStoreElem{
            qty: 10,
            makerCrankPendingQty: 5,
            priceLevelID: 3,
        });
        remove_price_qty_from_tree(&mut tree, 1, 5, false, vector[]);
        let pos = tree_find(&tree, 1);
        let (price, elem) = tree_get_mut(&mut tree, &pos);
        assert!(price == 1, 0);
        assert!(elem.qty == 5, 0);
        assert!(elem.makerCrankPendingQty == 5, 0);
        assert!(elem.priceLevelID == 3, 0);
        destroy_tree(tree);
    }

    #[test]
    fun test_unit_remove_price_qty_from_tree_non_crank_pos_hint() {
        let tree = new_tree<PriceStoreElem>(8);
        tree_insert(&mut tree, 1, PriceStoreElem{
            qty: 10,
            makerCrankPendingQty: 5,
            priceLevelID: 3,
        });
        let pos = tree_find(&tree, 1);
        remove_price_qty_from_tree(&mut tree, 1, 5, false, vector[pos]);
        let (price, elem) = tree_get_mut(&mut tree, &pos);
        assert!(price == 1, 0);
        assert!(elem.qty == 5, 0);
        assert!(elem.makerCrankPendingQty == 5, 0);
        assert!(elem.priceLevelID == 3, 0);
        destroy_tree(tree);
    }

    #[test]
    fun test_unit_remove_price_qty_from_tree_crank_no_pos_hint() {
        let tree = new_tree<PriceStoreElem>(8);
        tree_insert(&mut tree, 1, PriceStoreElem{
            qty: 10,
            makerCrankPendingQty: 5,
            priceLevelID: 3,
        });
        remove_price_qty_from_tree(&mut tree, 1, 2, true, vector[]);
        let pos = tree_find(&tree, 1);
        let (price, elem) = tree_get_mut(&mut tree, &pos);
        assert!(price == 1, 0);
        assert!(elem.qty == 10, 0);
        assert!(elem.makerCrankPendingQty == 3, 0);
        assert!(elem.priceLevelID == 3, 0);
        destroy_tree(tree);
    }

    #[test]
    fun test_unit_remove_price_qty_from_tree_crank_pos_hint() {
        let tree = new_tree<PriceStoreElem>(8);
        tree_insert(&mut tree, 1, PriceStoreElem{
            qty: 10,
            makerCrankPendingQty: 5,
            priceLevelID: 3,
        });
        let pos = tree_find(&tree, 1);
        remove_price_qty_from_tree(&mut tree, 1, 2, true, vector[pos]);
        let (price, elem) = tree_get_mut(&mut tree, &pos);
        assert!(price == 1, 0);
        assert!(elem.qty == 10, 0);
        assert!(elem.makerCrankPendingQty == 3, 0);
        assert!(elem.priceLevelID == 3, 0);
        destroy_tree(tree);
    }

    #[test]
    fun test_unit_remove_price_qty_from_tree_remove_price_store_elem() {
        let tree = new_tree<PriceStoreElem>(8);
        tree_insert(&mut tree, 1, PriceStoreElem{
            qty: 10,
            makerCrankPendingQty: 5,
            priceLevelID: 3,
        });
        remove_price_qty_from_tree(&mut tree, 1, 10, false, vector[]);
        let pos = tree_find(&tree, 1);
        let (price, elem) = tree_get_mut(&mut tree, &pos);
        assert!(price == 1, 0);
        assert!(elem.qty == 0, 0);
        assert!(elem.makerCrankPendingQty == 5, 0);
        assert!(elem.priceLevelID == 3, 0);
        remove_price_qty_from_tree(&mut tree, 1, 5, true, vector[]);
        let pos = tree_find(&tree, 1);
        assert!(pos.nodeID == 0, 0);
        destroy_tree(tree);
    }

    #[test]
    fun test_unit_remove_price_qty_from_cache_non_crank_no_pos_hint() {
        let cache = new_cache<PriceStoreElem>(SIDE_BUY);
        cache_insert(&mut cache, 1, PriceStoreElem{
            qty: 10,
            makerCrankPendingQty: 5,
            priceLevelID: 3,
        });
        remove_price_qty_from_cache(&mut cache, 1, 5, false, vector[]);
        let idx = *vector::borrow(&cache_find(&cache, 1), 0);
        let elem = vector::borrow(&cache.list, idx);
        assert!(elem.value.qty == 5, 0);
        assert!(elem.value.makerCrankPendingQty == 5, 0);
        assert!(elem.value.priceLevelID == 3, 0);
        drop_cache(cache);
    }

    #[test]
    fun test_unit_remove_price_qty_from_cache_non_crank_pos_hint() {
        let cache = new_cache<PriceStoreElem>(SIDE_SELL);
        cache_insert(&mut cache, 1, PriceStoreElem{
            qty: 10,
            makerCrankPendingQty: 5,
            priceLevelID: 3,
        });
        let idx = *vector::borrow(&cache_find(&cache, 1), 0);
        remove_price_qty_from_cache(&mut cache, 1, 5, false, vector[idx]);
        let elem = vector::borrow(&cache.list, idx);
        assert!(elem.value.qty == 5, 0);
        assert!(elem.value.makerCrankPendingQty == 5, 0);
        assert!(elem.value.priceLevelID == 3, 0);
        destroy_cache(cache);
    }

    #[test]
    fun test_unit_remove_price_qty_from_cache_crank_no_pos_hint() {
        let cache = new_cache<PriceStoreElem>(SIDE_BUY);
        cache_insert(&mut cache, 1, PriceStoreElem{
            qty: 10,
            makerCrankPendingQty: 5,
            priceLevelID: 3,
        });
        remove_price_qty_from_cache(&mut cache, 1, 2, true, vector[]);
        let idx = *vector::borrow(&cache_find(&cache, 1), 0);
        let elem = vector::borrow(&cache.list, idx);
        assert!(elem.value.qty == 10, 0);
        assert!(elem.value.makerCrankPendingQty == 3, 0);
        assert!(elem.value.priceLevelID == 3, 0);
        destroy_cache(cache);
    }

    #[test]
    fun test_unit_remove_price_qty_from_cache_crank_pos_hint() {
        let cache = new_cache<PriceStoreElem>(SIDE_BUY);
        cache_insert(&mut cache, 1, PriceStoreElem{
            qty: 10,
            makerCrankPendingQty: 5,
            priceLevelID: 3,
        });
        let idx = *vector::borrow(&cache_find(&cache, 1), 0);
        remove_price_qty_from_cache(&mut cache, 1, 2, true, vector[idx]);
        let elem = vector::borrow(&cache.list, idx);
        assert!(elem.value.qty == 10, 0);
        assert!(elem.value.makerCrankPendingQty == 3, 0);
        assert!(elem.value.priceLevelID == 3, 0);
        destroy_cache(cache);
    }

    #[test]
    fun test_unit_remove_price_qty_from_cache_remove_price_store_elem() {
        let cache = new_cache<PriceStoreElem>(SIDE_BUY);
        cache_insert(&mut cache, 1, PriceStoreElem{
            qty: 10,
            makerCrankPendingQty: 5,
            priceLevelID: 3,
        });
        remove_price_qty_from_cache(&mut cache, 1, 10, false, vector[]);
        let idx = *vector::borrow(&cache_find(&cache, 1), 0);
        let elem = vector::borrow(&cache.list, idx);
        assert!(elem.value.qty == 0, 0);
        assert!(elem.value.makerCrankPendingQty == 5, 0);
        assert!(elem.value.priceLevelID == 3, 0);
        remove_price_qty_from_cache(&mut cache, 1, 5, true, vector[]);
        let res = cache_find(&cache, 1);
        assert!(res == vector[], 0);
        destroy_cache(cache);
    }

    #[test]
    fun test_unit_remove_order_from_price_level() {
        let priceLevels = PriceLevelReuseTable{
            objects: table::new(),
            unusedStack: 0,
            currID: 1,
        };
        let orderTable = OrderReuseTable<FMA, FMB>{
            objects: table::new(),
            unusedStack: 0,
            currID: 1,
        };
        let priceLevelID = get_or_create_price_level(&mut priceLevels);
        let orderID1 = add_order_to_price_level(priceLevelID, &mut priceLevels, &mut orderTable, 10);
        let orderID2 = add_order_to_price_level(priceLevelID, &mut priceLevels, &mut orderTable, 20);
        let priceLevel = table::borrow(&priceLevels.objects, priceLevelID);
        assert!(get_price_level_order_qty(&priceLevel.orders, orderID1) == 10, 0);
        assert!(get_price_level_order_qty(&priceLevel.orders, orderID2) == 20, 0);

        let qtyRemoved  = remove_order_from_price_level(&mut priceLevels, priceLevelID, orderID2, 0);
        assert!(qtyRemoved == 20, 0);
        assert!(table::contains(&priceLevels.objects, priceLevelID), 0);
        let priceLevel = table::borrow(&priceLevels.objects, priceLevelID);
        assert!(get_price_level_order_qty(&priceLevel.orders, orderID1) == 10, 0);
        assert!(get_price_level_order_qty(&priceLevel.orders, orderID2) == 0, 0);

        let qtyRemoved  = remove_order_from_price_level(&mut priceLevels, priceLevelID, orderID1, 0);
        assert!(qtyRemoved == 10, 0);
        assert!(table::contains(&priceLevels.objects, priceLevelID), 0);
        let priceLevel = table::borrow(&priceLevels.objects, priceLevelID);
        assert!(get_price_level_order_qty(&priceLevel.orders, orderID1) == 0, 0);
        assert!(get_price_level_order_qty(&priceLevel.orders, orderID2) == 0, 0);
        assert!(!is_price_level_used(&priceLevels, priceLevelID), 0);

        drop_order_table(orderTable);
        drop_price_level_table(priceLevels);
    }

    #[test]
    fun test_unit_remove_order_from_price_level_valid_pending_crank_qty() {
        let priceLevels = PriceLevelReuseTable{
            objects: table::new(),
            unusedStack: 0,
            currID: 1,
        };
        let orderTable = OrderReuseTable<FMA, FMB>{
            objects: table::new(),
            unusedStack: 0,
            currID: 1,
        };
        let priceLevelID = get_or_create_price_level(&mut priceLevels);
        let orderID1 = add_order_to_price_level(priceLevelID, &mut priceLevels, &mut orderTable, 10);
        let orderID2 = add_order_to_price_level(priceLevelID, &mut priceLevels, &mut orderTable, 20);
        let priceLevel = table::borrow(&priceLevels.objects, priceLevelID);
        assert!(get_price_level_order_qty(&priceLevel.orders, orderID1) == 10, 0);
        assert!(get_price_level_order_qty(&priceLevel.orders, orderID2) == 20, 0);

        let qtyRemoved  = remove_order_from_price_level(&mut priceLevels, priceLevelID, orderID2, 2);
        assert!(qtyRemoved == 20, 0);
        assert!(table::contains(&priceLevels.objects, priceLevelID), 0);
        let priceLevel = table::borrow(&priceLevels.objects, priceLevelID);
        assert!(get_price_level_order_qty(&priceLevel.orders, orderID1) == 10, 0);
        assert!(get_price_level_order_qty(&priceLevel.orders, orderID2) == 0, 0);

        let qtyRemoved  = remove_order_from_price_level(&mut priceLevels, priceLevelID, orderID1, 2);
        assert!(qtyRemoved == 8, 0);
        assert!(table::contains(&priceLevels.objects, priceLevelID), 0);
        let priceLevel = table::borrow(&priceLevels.objects, priceLevelID);
        assert!(get_price_level_order_qty(&priceLevel.orders, orderID1) == 2, 0);
        assert!(get_price_level_order_qty(&priceLevel.orders, orderID2) == 0, 0);
        assert!(is_price_level_used(&priceLevels, priceLevelID), 0);

        drop_order_table(orderTable);
        drop_price_level_table(priceLevels);
    }

    #[test]
    fun test_unit_remove_order_from_price_level_valid_pending_crank_qty_multiple_orders() {
        let priceLevels = PriceLevelReuseTable{
            objects: table::new(),
            unusedStack: 0,
            currID: 1,
        };
        let orderTable = OrderReuseTable<FMA, FMB>{
            objects: table::new(),
            unusedStack: 0,
            currID: 1,
        };
        let priceLevelID = get_or_create_price_level(&mut priceLevels);
        let orderID1 = add_order_to_price_level(priceLevelID, &mut priceLevels, &mut orderTable, 10);
        let orderID2 = add_order_to_price_level(priceLevelID, &mut priceLevels, &mut orderTable, 20);
        let priceLevel = table::borrow(&priceLevels.objects, priceLevelID);
        assert!(get_price_level_order_qty(&priceLevel.orders, orderID1) == 10, 0);
        assert!(get_price_level_order_qty(&priceLevel.orders, orderID2) == 20, 0);

        let qtyRemoved  = remove_order_from_price_level(&mut priceLevels, priceLevelID, orderID2, 15);
        assert!(qtyRemoved == 15, 0);
        assert!(table::contains(&priceLevels.objects, priceLevelID), 0);
        let priceLevel = table::borrow(&priceLevels.objects, priceLevelID);
        assert!(get_price_level_order_qty(&priceLevel.orders, orderID1) == 10, 0);
        assert!(get_price_level_order_qty(&priceLevel.orders, orderID2) == 5, 0);

        drop_order_table(orderTable);
        drop_price_level_table(priceLevels);
    }

    #[test]
    #[expected_failure(abort_code=ERR_ORDER_EXECUTED_BUT_IS_PENDING_CRANK)]
    fun test_unit_remove_order_from_price_level_too_much_pending_crank_qty() {
        let priceLevels = PriceLevelReuseTable{
            objects: table::new(),
            unusedStack: 0,
            currID: 1,
        };
        let orderTable = OrderReuseTable<FMA, FMB>{
            objects: table::new(),
            unusedStack: 0,
            currID: 1,
        };
        let priceLevelID = get_or_create_price_level(&mut priceLevels);
        let orderID1 = add_order_to_price_level(priceLevelID, &mut priceLevels, &mut orderTable, 10);
        let orderID2 = add_order_to_price_level(priceLevelID, &mut priceLevels, &mut orderTable, 20);
        let priceLevel = table::borrow(&priceLevels.objects, priceLevelID);
        assert!(get_price_level_order_qty(&priceLevel.orders, orderID1) == 10, 0);
        assert!(get_price_level_order_qty(&priceLevel.orders, orderID2) == 20, 0);

        remove_order_from_price_level(&mut priceLevels, priceLevelID, orderID1, 15);

        drop_order_table(orderTable);
        drop_price_level_table(priceLevels);
    }

    #[test_only]
    fun add_order_to_price_level<I, Q>(
        priceLevelID: u16,
        priceLevelTable: &mut PriceLevelReuseTable,
        orderTable: &mut OrderReuseTable<I, Q>,
        qty: u64,
    ): u32 {
        let orderID = get_or_create_order(orderTable);
        let order = table::borrow_mut(&mut orderTable.objects, orderID);
        let priceLevel = table::borrow_mut(&mut priceLevelTable.objects, priceLevelID);
        list_push(&mut priceLevel.orders, PriceLevelOrder {
            id: orderID,
            qty,
        });
        order.priceLevelID = priceLevelID;
        order.metadata.originalQty = qty;
        order.metadata.unfilledQty = qty;
        orderID
    }

    #[test_only]
    fun drop_price_level_table(priceLevels: PriceLevelReuseTable) {
        let PriceLevelReuseTable {
            objects,
            unusedStack: _,
            currID: _,
        } = priceLevels;
        table::drop_unchecked(objects);
    }

    #[test_only]
    fun get_price_level_order_qty(list: &NodeList<PriceLevelOrder>, orderID: u32): u64 {
        let it = list_iterate(list);
        while (it.nodeID != 0) {
            let elem = list_get_next(list, &mut it);
            if (elem.id == orderID) {
                return elem.qty
            }
        };
        return 0
    }

    #[test_only]
    fun drop_order_table<I, Q>(orders: OrderReuseTable<I, Q>) {
        let OrderReuseTable {
            objects,
            unusedStack: _,
            currID: _,
        } = orders;
        table::drop_unchecked(objects);
    }

    #[test_only]
    fun drop_cache<T: drop + store>(cache: Cache<T>) {
        let Cache {
            side: _,
            list,
        } = cache;
        while (!vector::is_empty(&list)) {
            vector::pop_back(&mut list);
        };
        vector::destroy_empty(list);
    }

    #[test_only]
    fun is_price_level_used(priceLevels: &PriceLevelReuseTable, priceLevelID: u16): bool {
        let currNode = priceLevels.unusedStack;
        while (currNode != 0) {
            if (currNode == priceLevelID) {
                return false
            };
            let priceLevel = table::borrow(&priceLevels.objects, priceLevelID);
            currNode = priceLevel.next;
        };
        true
    }

    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="Crank tests">

    #[test(aptos=@0x1, ferum=@ferum, user1=@0x3, makerUser=@0x4, takerUser=@0x6)]
    fun test_market_crank_not_finalize_maker_multiple_in_level_cache_buy(
        aptos: &signer,
        ferum: &signer,
        user1: &signer,
        makerUser: &signer,
        takerUser: &signer,
    )
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that an execution event which executes a single maker partially is handled properly.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user1, 2);
        account::create_account_for_test(address_of(makerUser));
        deposit_fake_coins(ferum, 10000000000, makerUser);
        account::create_account_for_test(address_of(takerUser));
        deposit_fake_coins(ferum, 10000000000, takerUser);
        let makerAccID = platform::account_identifier_for_test(makerUser);
        let makerAccKey = open_market_account<FMA, FMB>(makerUser, vector[makerAccID]);
        deposit_to_market_account<FMA, FMB>(makerUser, makerAccKey, 1000000000000, 1000000000000);
        let takerAccID = platform::account_identifier_for_test(takerUser);
        let takerAccKey = open_market_account<FMA, FMB>(takerUser, vector[takerAccID]);
        deposit_to_market_account<FMA, FMB>(takerUser, takerAccKey, 1000000000000, 1000000000000);

        // Setup.
        add_user_limit_order<FMA, FMB>(user1, SIDE_BUY, BEHAVIOUR_GTC, 8000000000, 80000000000);
        let makerID = add_user_limit_order<FMA, FMB>(makerUser, SIDE_BUY, BEHAVIOUR_GTC, 9000000000, 70000000000);
        add_user_limit_order<FMA, FMB>(user1, SIDE_BUY, BEHAVIOUR_GTC, 9000000000, 60000000000);
        add_user_limit_order<FMA, FMB>(user1, SIDE_BUY, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user1, SIDE_BUY, BEHAVIOUR_GTC, 6000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user1, SIDE_BUY, BEHAVIOUR_GTC, 5000000000, 30000000000);
        let takerID = add_user_limit_order<FMA, FMB>(takerUser, SIDE_SELL, BEHAVIOUR_GTC, 5500000000, 30000000000);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.5 qty: 3, crankQty: 0)"),
            s(b"(0.6 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
            s(b"(0.8 qty: 8, crankQty: 0)"),
            s(b"(0.9 qty: 10, crankQty: 3)"),
        ]);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[], vector[]);
        assert_exec_events<FMA, FMB>(marketAddr, vector[
            ExecEventInfo {
                qty: 30000000000,
                takerOrderID: takerID,
                price: 9000000000,
            },
        ]);
        assert_order_qtys<FMA, FMB>(marketAddr, makerID, 70000000000, 0, 30000000000);
        assert_order_qtys<FMA, FMB>(marketAddr, takerID, 30000000000, 30000000000, 0);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert_order_collateral(book, takerID, 0, 30000000000);
        assert_order_collateral(book, makerID, 63000000000, 0);
        assert_order_used(book, takerID);
        assert_order_used(book, makerID);
        assert_account_balances(book, makerAccKey, 1000000000000, 937000000000);
        assert_account_balances(book, takerAccKey, 970000000000, 1000000000000);
        assert!(book.summary.buyCacheMax == 9000000000, 0);
        assert!(book.summary.buyCacheMin == 8000000000, 0);
        assert!(book.summary.buyCacheQty == 180000000000, 0);
        assert!(book.summary.buyCacheSize == 2, 0);
        assert!(book.summary.buyTreeMax == 7000000000, 0);

        // Crank.
        crank<FMA, FMB>(user1, 1);
        assert_exec_events<FMA, FMB>(marketAddr, vector[]);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.5 qty: 3, crankQty: 0)"),
            s(b"(0.6 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
            s(b"(0.8 qty: 8, crankQty: 0)"),
            s(b"(0.9 qty: 10, crankQty: 0)"),
        ]);
        assert_order_qtys<FMA, FMB>(marketAddr, takerID, 0, 0, 0); // No longer used.
        assert_order_qtys<FMA, FMB>(marketAddr, makerID, 40000000000, 0, 0);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert_order_collateral(book, makerID, 36000000000, 0);
        assert_order_collateral(book, takerID, 0, 0);
        assert_order_unused(book, takerID);
        assert_order_used(book, makerID);
        assert_account_balances(book, makerAccKey, 1030000000000, 937000000000);
        assert_account_balances(book, takerAccKey, 970000000000, 1027000000000);
        assert!(book.summary.buyCacheMax == 9000000000, 0);
        assert!(book.summary.buyCacheMin == 8000000000, 0);
        assert!(book.summary.buyCacheQty == 180000000000, 0);
        assert!(book.summary.buyCacheSize == 2, 0);
        assert!(book.summary.buyTreeMax == 7000000000, 0);
    }

    #[test(aptos=@0x1, ferum=@ferum, user1=@0x3, makerUser=@0x4, takerUser=@0x6)]
    fun test_market_crank_not_finalize_maker_multiple_in_level_tree_buy(
        aptos: &signer,
        ferum: &signer,
        user1: &signer,
        makerUser: &signer,
        takerUser: &signer,
    )
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that an execution event which executes a single maker partially is handled properly.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user1, 2);
        account::create_account_for_test(address_of(makerUser));
        deposit_fake_coins(ferum, 10000000000, makerUser);
        account::create_account_for_test(address_of(takerUser));
        deposit_fake_coins(ferum, 10000000000, takerUser);
        let makerAccID = platform::account_identifier_for_test(makerUser);
        let makerAccKey = open_market_account<FMA, FMB>(makerUser, vector[makerAccID]);
        deposit_to_market_account<FMA, FMB>(makerUser, makerAccKey, 1000000000000, 1000000000000);
        let takerAccID = platform::account_identifier_for_test(takerUser);
        let takerAccKey = open_market_account<FMA, FMB>(takerUser, vector[takerAccID]);
        deposit_to_market_account<FMA, FMB>(takerUser, takerAccKey, 1000000000000, 1000000000000);

        // Setup.
        let orderID1 = add_user_limit_order<FMA, FMB>(user1, SIDE_BUY, BEHAVIOUR_GTC, 8000000000, 80000000000);
        let orderID2 = add_user_limit_order<FMA, FMB>(user1, SIDE_BUY, BEHAVIOUR_GTC, 9000000000, 70000000000);
        let makerID =  add_user_limit_order<FMA, FMB>(makerUser, SIDE_BUY, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user1, SIDE_BUY, BEHAVIOUR_GTC, 7000000000, 60000000000);
        add_user_limit_order<FMA, FMB>(user1, SIDE_BUY, BEHAVIOUR_GTC, 6000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user1, SIDE_BUY, BEHAVIOUR_GTC, 5000000000, 30000000000);
        cancel_order<FMA, FMB>(user1, orderID1);
        cancel_order<FMA, FMB>(user1, orderID2);
        let takerID = add_user_limit_order<FMA, FMB>(takerUser, SIDE_SELL, BEHAVIOUR_GTC, 5500000000, 30000000000);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.5 qty: 3, crankQty: 0)"),
            s(b"(0.6 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 8, crankQty: 3)"),
        ], vector[]);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[], vector[]);
        assert_exec_events<FMA, FMB>(marketAddr, vector[
            ExecEventInfo {
                qty: 30000000000,
                takerOrderID: takerID,
                price: 7000000000,
            },
        ]);
        assert_order_qtys<FMA, FMB>(marketAddr, makerID, 50000000000, 0, 30000000000);
        assert_order_qtys<FMA, FMB>(marketAddr, takerID, 30000000000, 30000000000, 0);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert_order_collateral(book, takerID, 0, 30000000000);
        assert_order_collateral(book, makerID, 35000000000, 0);
        assert_order_used(book, takerID);
        assert_order_used(book, makerID);
        assert_account_balances(book, makerAccKey, 1000000000000, 965000000000);
        assert_account_balances(book, takerAccKey, 970000000000, 1000000000000);
        assert!(book.summary.buyCacheMax == 0, 0);
        assert!(book.summary.buyCacheMin == 0, 0);
        assert!(book.summary.buyCacheQty == 0, 0);
        assert!(book.summary.buyCacheSize == 0, 0);
        assert!(book.summary.buyTreeMax == 7000000000, 0);

        // Crank.
        crank<FMA, FMB>(user1, 1);
        assert_exec_events<FMA, FMB>(marketAddr, vector[]);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.5 qty: 3, crankQty: 0)"),
            s(b"(0.6 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 8, crankQty: 0)"),
        ], vector[]);
        assert_order_qtys<FMA, FMB>(marketAddr, takerID, 0, 0, 0); // No longer used.
        assert_order_qtys<FMA, FMB>(marketAddr, makerID, 20000000000, 0, 0);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert_order_collateral(book, makerID, 14000000000, 0);
        assert_order_collateral(book, takerID, 0, 0);
        assert_order_unused(book, takerID);
        assert_order_used(book, makerID);
        assert_account_balances(book, makerAccKey, 1030000000000, 965000000000);
        assert_account_balances(book, takerAccKey, 970000000000, 1021000000000);
        assert!(book.summary.buyCacheMax == 0, 0);
        assert!(book.summary.buyCacheMin == 0, 0);
        assert!(book.summary.buyCacheQty == 0, 0);
        assert!(book.summary.buyCacheSize == 0, 0);
        assert!(book.summary.buyTreeMax == 7000000000, 0);
    }

    #[test(aptos=@0x1, ferum=@ferum, user1=@0x3, makerUser=@0x4, takerUser=@0x6)]
    fun test_market_crank_not_finalize_maker_single_in_level_cache_buy(
        aptos: &signer,
        ferum: &signer,
        user1: &signer,
        makerUser: &signer,
        takerUser: &signer,
    )
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that an execution event which executes a single maker partially is handled properly.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user1, 2);
        account::create_account_for_test(address_of(makerUser));
        deposit_fake_coins(ferum, 10000000000, makerUser);
        account::create_account_for_test(address_of(takerUser));
        deposit_fake_coins(ferum, 10000000000, takerUser);
        let makerAccID = platform::account_identifier_for_test(makerUser);
        let makerAccKey = open_market_account<FMA, FMB>(makerUser, vector[makerAccID]);
        deposit_to_market_account<FMA, FMB>(makerUser, makerAccKey, 1000000000000, 1000000000000);
        let takerAccID = platform::account_identifier_for_test(takerUser);
        let takerAccKey = open_market_account<FMA, FMB>(takerUser, vector[takerAccID]);
        deposit_to_market_account<FMA, FMB>(takerUser, takerAccKey, 1000000000000, 1000000000000);

        // Setup.
        add_user_limit_order<FMA, FMB>(user1, SIDE_BUY, BEHAVIOUR_GTC, 8000000000, 80000000000);
        let makerID = add_user_limit_order<FMA, FMB>(makerUser, SIDE_BUY, BEHAVIOUR_GTC, 9000000000, 70000000000);
        add_user_limit_order<FMA, FMB>(user1, SIDE_BUY, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user1, SIDE_BUY, BEHAVIOUR_GTC, 6000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user1, SIDE_BUY, BEHAVIOUR_GTC, 5000000000, 30000000000);
        let takerID = add_user_limit_order<FMA, FMB>(takerUser, SIDE_SELL, BEHAVIOUR_GTC, 5500000000, 30000000000);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.5 qty: 3, crankQty: 0)"),
            s(b"(0.6 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
            s(b"(0.8 qty: 8, crankQty: 0)"),
            s(b"(0.9 qty: 4, crankQty: 3)"),
        ]);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[], vector[]);
        assert_exec_events<FMA, FMB>(marketAddr, vector[
            ExecEventInfo {
                qty: 30000000000,
                takerOrderID: takerID,
                price: 9000000000,
            },
        ]);
        assert_order_qtys<FMA, FMB>(marketAddr, makerID, 70000000000, 0, 30000000000);
        assert_order_qtys<FMA, FMB>(marketAddr, takerID, 30000000000, 30000000000, 0);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert_order_collateral(book, takerID, 0, 30000000000);
        assert_order_collateral(book, makerID, 63000000000, 0);
        assert_order_used(book, takerID);
        assert_order_used(book, makerID);
        assert_account_balances(book, makerAccKey, 1000000000000, 937000000000);
        assert_account_balances(book, takerAccKey, 970000000000, 1000000000000);
        assert!(book.summary.buyCacheMax == 9000000000, 0);
        assert!(book.summary.buyCacheMin == 8000000000, 0);
        assert!(book.summary.buyCacheQty == 120000000000, 0);
        assert!(book.summary.buyCacheSize == 2, 0);
        assert!(book.summary.buyTreeMax == 7000000000, 0);

        // Crank.
        crank<FMA, FMB>(user1, 1);
        assert_exec_events<FMA, FMB>(marketAddr, vector[]);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.5 qty: 3, crankQty: 0)"),
            s(b"(0.6 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
            s(b"(0.8 qty: 8, crankQty: 0)"),
            s(b"(0.9 qty: 4, crankQty: 0)"),
        ]);
        assert_order_qtys<FMA, FMB>(marketAddr, takerID, 0, 0, 0); // No longer used.
        assert_order_qtys<FMA, FMB>(marketAddr, makerID, 40000000000, 0, 0);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert_order_collateral(book, makerID, 36000000000, 0);
        assert_order_collateral(book, takerID, 0, 0);
        assert_order_unused(book, takerID);
        assert_order_used(book, makerID);
        assert_account_balances(book, makerAccKey, 1030000000000, 937000000000);
        assert_account_balances(book, takerAccKey, 970000000000, 1027000000000);
        assert!(book.summary.buyCacheMax == 9000000000, 0);
        assert!(book.summary.buyCacheMin == 8000000000, 0);
        assert!(book.summary.buyCacheQty == 120000000000, 0);
        assert!(book.summary.buyCacheSize == 2, 0);
        assert!(book.summary.buyTreeMax == 7000000000, 0);
    }

    #[test(aptos=@0x1, ferum=@ferum, user1=@0x3, makerUser=@0x4, takerUser=@0x6)]
    fun test_market_crank_not_finalize_maker_single_in_level_tree_buy(
        aptos: &signer,
        ferum: &signer,
        user1: &signer,
        makerUser: &signer,
        takerUser: &signer,
    )
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that an execution event which executes a single maker partially is handled properly.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user1, 2);
        account::create_account_for_test(address_of(makerUser));
        deposit_fake_coins(ferum, 10000000000, makerUser);
        account::create_account_for_test(address_of(takerUser));
        deposit_fake_coins(ferum, 10000000000, takerUser);
        let makerAccID = platform::account_identifier_for_test(makerUser);
        let makerAccKey = open_market_account<FMA, FMB>(makerUser, vector[makerAccID]);
        deposit_to_market_account<FMA, FMB>(makerUser, makerAccKey, 1000000000000, 1000000000000);
        let takerAccID = platform::account_identifier_for_test(takerUser);
        let takerAccKey = open_market_account<FMA, FMB>(takerUser, vector[takerAccID]);
        deposit_to_market_account<FMA, FMB>(takerUser, takerAccKey, 1000000000000, 1000000000000);

        // Setup.
        let orderID1 = add_user_limit_order<FMA, FMB>(user1, SIDE_BUY, BEHAVIOUR_GTC, 8000000000, 80000000000);
        let orderID2 = add_user_limit_order<FMA, FMB>(user1, SIDE_BUY, BEHAVIOUR_GTC, 9000000000, 70000000000);
        let makerID =  add_user_limit_order<FMA, FMB>(makerUser, SIDE_BUY, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user1, SIDE_BUY, BEHAVIOUR_GTC, 6000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user1, SIDE_BUY, BEHAVIOUR_GTC, 5000000000, 30000000000);
        cancel_order<FMA, FMB>(user1, orderID1);
        cancel_order<FMA, FMB>(user1, orderID2);
        let takerID = add_user_limit_order<FMA, FMB>(takerUser, SIDE_SELL, BEHAVIOUR_GTC, 5500000000, 30000000000);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.5 qty: 3, crankQty: 0)"),
            s(b"(0.6 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 2, crankQty: 3)"),
        ], vector[]);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[], vector[]);
        assert_exec_events<FMA, FMB>(marketAddr, vector[
            ExecEventInfo {
                qty: 30000000000,
                takerOrderID: takerID,
                price: 7000000000,
            },
        ]);
        assert_order_qtys<FMA, FMB>(marketAddr, makerID, 50000000000, 0, 30000000000);
        assert_order_qtys<FMA, FMB>(marketAddr, takerID, 30000000000, 30000000000, 0);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert_order_collateral(book, takerID, 0, 30000000000);
        assert_order_collateral(book, makerID, 35000000000, 0);
        assert_order_used(book, takerID);
        assert_order_used(book, makerID);
        assert_account_balances(book, makerAccKey, 1000000000000, 965000000000);
        assert_account_balances(book, takerAccKey, 970000000000, 1000000000000);
        assert!(book.summary.buyCacheMax == 0, 0);
        assert!(book.summary.buyCacheMin == 0, 0);
        assert!(book.summary.buyCacheQty == 0, 0);
        assert!(book.summary.buyCacheSize == 0, 0);
        assert!(book.summary.buyTreeMax == 7000000000, 0);

        // Crank.
        crank<FMA, FMB>(user1, 1);
        assert_exec_events<FMA, FMB>(marketAddr, vector[]);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.5 qty: 3, crankQty: 0)"),
            s(b"(0.6 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 2, crankQty: 0)"),
        ], vector[]);
        assert_order_qtys<FMA, FMB>(marketAddr, takerID, 0, 0, 0); // No longer used.
        assert_order_qtys<FMA, FMB>(marketAddr, makerID, 20000000000, 0, 0);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert_order_collateral(book, makerID, 14000000000, 0);
        assert_order_collateral(book, takerID, 0, 0);
        assert_order_unused(book, takerID);
        assert_order_used(book, makerID);
        assert_account_balances(book, makerAccKey, 1030000000000, 965000000000);
        assert_account_balances(book, takerAccKey, 970000000000, 1021000000000);
        assert!(book.summary.buyCacheMax == 0, 0);
        assert!(book.summary.buyCacheMin == 0, 0);
        assert!(book.summary.buyCacheQty == 0, 0);
        assert!(book.summary.buyCacheSize == 0, 0);
        assert!(book.summary.buyTreeMax == 7000000000, 0);
    }

    #[test(aptos=@0x1, ferum=@ferum, user1=@0x3, makerUser=@0x4, takerUser=@0x6)]
    fun test_market_crank_not_finalize_taker_buy(
        aptos: &signer,
        ferum: &signer,
        user1: &signer,
        makerUser: &signer,
        takerUser: &signer,
    )
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that an execution event which executes a single maker partially is handled properly.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user1, 2);
        account::create_account_for_test(address_of(makerUser));
        deposit_fake_coins(ferum, 10000000000, makerUser);
        account::create_account_for_test(address_of(takerUser));
        deposit_fake_coins(ferum, 10000000000, takerUser);
        let makerAccID = platform::account_identifier_for_test(makerUser);
        let makerAccKey = open_market_account<FMA, FMB>(makerUser, vector[makerAccID]);
        deposit_to_market_account<FMA, FMB>(makerUser, makerAccKey, 1000000000000, 1000000000000);
        let takerAccID = platform::account_identifier_for_test(takerUser);
        let takerAccKey = open_market_account<FMA, FMB>(takerUser, vector[takerAccID]);
        deposit_to_market_account<FMA, FMB>(takerUser, takerAccKey, 1000000000000, 1000000000000);

        // Setup.
        add_user_limit_order<FMA, FMB>(user1, SIDE_BUY, BEHAVIOUR_GTC, 8000000000, 80000000000);
        let makerID = add_user_limit_order<FMA, FMB>(makerUser, SIDE_BUY, BEHAVIOUR_GTC, 9000000000, 70000000000);
        add_user_limit_order<FMA, FMB>(user1, SIDE_BUY, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user1, SIDE_BUY, BEHAVIOUR_GTC, 6000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user1, SIDE_BUY, BEHAVIOUR_GTC, 5000000000, 30000000000);
        let takerID = add_user_limit_order<FMA, FMB>(takerUser, SIDE_SELL, BEHAVIOUR_GTC, 8500000000, 80000000000);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.5 qty: 3, crankQty: 0)"),
            s(b"(0.6 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
            s(b"(0.8 qty: 8, crankQty: 0)"),
            s(b"(0.9 qty: 0, crankQty: 7)"),
        ]);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[], vector[
            s(b"(0.85 qty: 1, crankQty: 0)"),
        ]);
        assert_exec_events<FMA, FMB>(marketAddr, vector[
            ExecEventInfo {
                qty: 70000000000,
                takerOrderID: takerID,
                price: 9000000000,
            },
        ]);
        assert_order_qtys<FMA, FMB>(marketAddr, makerID, 70000000000, 0, 70000000000);
        assert_order_qtys<FMA, FMB>(marketAddr, takerID, 80000000000, 70000000000, 0);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert_order_collateral(book, takerID, 0, 80000000000);
        assert_order_collateral(book, makerID, 63000000000, 0);
        assert_order_used(book, takerID);
        assert_order_used(book, makerID);
        assert_account_balances(book, makerAccKey, 1000000000000, 937000000000);
        assert_account_balances(book, takerAccKey, 920000000000, 1000000000000);
        assert!(book.summary.buyCacheMax == 9000000000, 0);
        assert!(book.summary.buyCacheMin == 8000000000, 0);
        assert!(book.summary.buyCacheQty == 80000000000, 0);
        assert!(book.summary.buyCacheSize == 2, 0);
        assert!(book.summary.buyTreeMax == 7000000000, 0);
        assert!(book.summary.sellCacheMax == 8500000000, 0);
        assert!(book.summary.sellCacheMin == 8500000000, 0);
        assert!(book.summary.sellCacheQty == 10000000000, 0);
        assert!(book.summary.sellCacheSize == 1, 0);
        assert!(book.summary.sellTreeMin == 0, 0);

        // Crank.
        crank<FMA, FMB>(user1, 1);
        assert_exec_events<FMA, FMB>(marketAddr, vector[]);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.5 qty: 3, crankQty: 0)"),
            s(b"(0.6 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
            s(b"(0.8 qty: 8, crankQty: 0)"),
        ]);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[], vector[
            s(b"(0.85 qty: 1, crankQty: 0)"),
        ]);
        assert_order_qtys<FMA, FMB>(marketAddr, takerID, 10000000000, 0, 0);
        assert_order_qtys<FMA, FMB>(marketAddr, makerID, 0, 0, 0); // No longer used.
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert_order_collateral(book, makerID, 0, 0);
        assert_order_collateral(book, takerID, 0, 10000000000);
        assert_order_used(book, takerID);
        assert_order_unused(book, makerID);
        assert_account_balances(book, makerAccKey, 1070000000000, 937000000000);
        assert_account_balances(book, takerAccKey, 920000000000, 1063000000000);
        assert!(book.summary.buyCacheMax == 8000000000, 0);
        assert!(book.summary.buyCacheMin == 8000000000, 0);
        assert!(book.summary.buyCacheQty == 80000000000, 0);
        assert!(book.summary.buyCacheSize == 1, 0);
        assert!(book.summary.buyTreeMax == 7000000000, 0);
        assert!(book.summary.sellCacheMax == 8500000000, 0);
        assert!(book.summary.sellCacheMin == 8500000000, 0);
        assert!(book.summary.sellCacheQty == 10000000000, 0);
        assert!(book.summary.sellCacheSize == 1, 0);
        assert!(book.summary.sellTreeMin == 0, 0);
    }

    #[test(aptos=@0x1, ferum=@ferum, user1=@0x3, makerUser1=@0x4, makerUser2=@0x5, takerUser=@0x6)]
    fun test_market_crank_execute_against_multiple_makers_buy(
        aptos: &signer,
        ferum: &signer,
        user1: &signer,
        makerUser1: &signer,
        makerUser2: &signer,
        takerUser: &signer,
    )
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that an execution event which execute against multiple makers is processed correctly.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user1, 2);
        account::create_account_for_test(address_of(makerUser1));
        deposit_fake_coins(ferum, 10000000000, makerUser1);
        account::create_account_for_test(address_of(makerUser2));
        deposit_fake_coins(ferum, 10000000000, makerUser2);
        account::create_account_for_test(address_of(takerUser));
        deposit_fake_coins(ferum, 10000000000, takerUser);
        let maker1AccID = platform::account_identifier_for_test(makerUser1);
        let maker1AccKey = open_market_account<FMA, FMB>(makerUser1, vector[maker1AccID]);
        deposit_to_market_account<FMA, FMB>(makerUser1, maker1AccKey, 1000000000000, 1000000000000);
        let maker2AccID = platform::account_identifier_for_test(makerUser2);
        let maker2AccKey = open_market_account<FMA, FMB>(makerUser2, vector[maker2AccID]);
        deposit_to_market_account<FMA, FMB>(makerUser2, maker2AccKey, 1000000000000, 1000000000000);
        let takerAccID = platform::account_identifier_for_test(takerUser);
        let takerAccKey = open_market_account<FMA, FMB>(takerUser, vector[takerAccID]);
        deposit_to_market_account<FMA, FMB>(takerUser, takerAccKey, 1000000000000, 1000000000000);

        // Setup.
        let makerID3 = add_user_limit_order<FMA, FMB>(makerUser2, SIDE_BUY, BEHAVIOUR_GTC, 8000000000, 80000000000);
        let makerID1 = add_user_limit_order<FMA, FMB>(makerUser1, SIDE_BUY, BEHAVIOUR_GTC, 9000000000, 70000000000);
        let makerID2 = add_user_limit_order<FMA, FMB>(makerUser1, SIDE_BUY, BEHAVIOUR_GTC, 9000000000, 60000000000);
        let makerID4 = add_user_limit_order<FMA, FMB>(makerUser2, SIDE_BUY, BEHAVIOUR_GTC, 7000000000, 50000000000);
        let makerID5 = add_user_limit_order<FMA, FMB>(makerUser2, SIDE_BUY, BEHAVIOUR_GTC, 6000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user1, SIDE_BUY, BEHAVIOUR_GTC, 5000000000, 30000000000);
        let takerID = add_user_limit_order<FMA, FMB>(takerUser, SIDE_SELL, BEHAVIOUR_GTC, 5500000000, 300000000000);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.5 qty: 3, crankQty: 0)"),
            s(b"(0.6 qty: 0, crankQty: 4)"),
            s(b"(0.7 qty: 0, crankQty: 5)"),
        ], vector[
            s(b"(0.8 qty: 0, crankQty: 8)"),
            s(b"(0.9 qty: 0, crankQty: 13)"),
        ]);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[], vector[]);
        assert_exec_events<FMA, FMB>(marketAddr, vector[
            ExecEventInfo {
                qty: 130000000000,
                takerOrderID: takerID,
                price: 9000000000,
            },
            ExecEventInfo {
                qty: 80000000000,
                takerOrderID: takerID,
                price: 8000000000,
            },
            ExecEventInfo {
                qty: 50000000000,
                takerOrderID: takerID,
                price: 7000000000,
            },
            ExecEventInfo {
                qty: 40000000000,
                takerOrderID: takerID,
                price: 6000000000,
            },
        ]);
        assert_order_qtys<FMA, FMB>(marketAddr, makerID1, 70000000000, 0, 70000000000);
        assert_order_qtys<FMA, FMB>(marketAddr, makerID2, 60000000000, 0, 60000000000);
        assert_order_qtys<FMA, FMB>(marketAddr, makerID3, 80000000000, 0, 80000000000);
        assert_order_qtys<FMA, FMB>(marketAddr, makerID4, 50000000000, 0, 50000000000);
        assert_order_qtys<FMA, FMB>(marketAddr, makerID5, 40000000000, 0, 40000000000);
        assert_order_qtys<FMA, FMB>(marketAddr, takerID, 300000000000, 300000000000, 0);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert_order_collateral(book, takerID, 0, 300000000000);
        assert_order_collateral(book, makerID1, 63000000000, 0);
        assert_order_collateral(book, makerID2, 54000000000, 0);
        assert_order_collateral(book, makerID3, 64000000000, 0);
        assert_order_collateral(book, makerID4, 35000000000, 0);
        assert_order_collateral(book, makerID5, 24000000000, 0);
        assert_order_used(book, takerID);
        assert_order_used(book, makerID1);
        assert_order_used(book, makerID2);
        assert_order_used(book, makerID3);
        assert_order_used(book, makerID4);
        assert_order_used(book, makerID5);
        assert_account_balances(book, maker1AccKey, 1000000000000, 883000000000);
        assert_account_balances(book, maker2AccKey, 1000000000000, 877000000000);
        assert_account_balances(book, takerAccKey, 700000000000, 1000000000000);
        assert!(book.summary.buyCacheMax == 9000000000, 0);
        assert!(book.summary.buyCacheMin == 8000000000, 0);
        assert!(book.summary.buyCacheQty == 0, 0);
        assert!(book.summary.buyCacheSize == 2, 0);
        assert!(book.summary.buyTreeMax == 7000000000, 0);

        // Crank.
        crank<FMA, FMB>(user1, 1);
        assert_exec_events<FMA, FMB>(marketAddr, vector[]);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.5 qty: 3, crankQty: 0)"),
        ], vector[]);
        assert_order_qtys<FMA, FMB>(marketAddr, makerID1, 0, 0, 0); // No longer used.
        assert_order_qtys<FMA, FMB>(marketAddr, makerID2, 0, 0, 0); // No longer used.
        assert_order_qtys<FMA, FMB>(marketAddr, makerID3, 0, 0, 0); // No longer used.
        assert_order_qtys<FMA, FMB>(marketAddr, makerID4, 0, 0, 0); // No longer used.
        assert_order_qtys<FMA, FMB>(marketAddr, makerID5, 0, 0, 0); // No longer used.
        assert_order_qtys<FMA, FMB>(marketAddr, takerID, 0, 0, 0); // No longer used.
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert_order_collateral(book, takerID, 0, 0);
        assert_order_collateral(book, makerID1, 0, 0);
        assert_order_collateral(book, makerID2, 0, 0);
        assert_order_collateral(book, makerID3, 0, 0);
        assert_order_collateral(book, makerID4, 0, 0);
        assert_order_collateral(book, makerID5, 0, 0);
        assert_order_unused(book, makerID1);
        assert_order_unused(book, makerID2);
        assert_order_unused(book, makerID3);
        assert_order_unused(book, makerID4);
        assert_order_unused(book, makerID5);
        assert_order_unused(book, takerID);
        assert_account_balances(book, maker1AccKey, 1130000000000, 883000000000);
        assert_account_balances(book, maker2AccKey, 1170000000000, 877000000000);
        assert_account_balances(book, takerAccKey, 700000000000, 1240000000000);
        assert!(book.summary.buyCacheMax == 0, 0);
        assert!(book.summary.buyCacheMin == 0, 0);
        assert!(book.summary.buyCacheQty == 0, 0);
        assert!(book.summary.buyCacheSize == 0, 0);
        assert!(book.summary.buyTreeMax == 5000000000, 0);
    }

    #[test(aptos=@0x1, ferum=@ferum, user1=@0x3, user2=@0x4, user3=@0x5)]
    fun test_market_crank_qty_in_cache_buy(aptos: &signer, ferum: &signer, user1: &signer, user2: &signer, user3: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that an execution event for qty in the cache is processed correctly.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user1, 2);
        account::create_account_for_test(address_of(user2));
        deposit_fake_coins(ferum, 10000000000, user2);
        account::create_account_for_test(address_of(user3));
        deposit_fake_coins(ferum, 10000000000, user3);
        let user2AccID = platform::account_identifier_for_test(user2);
        let user2AccKey = open_market_account<FMA, FMB>(user2, vector[user2AccID]);
        deposit_to_market_account<FMA, FMB>(user2, user2AccKey, 1000000000000, 1000000000000);
        let user3AccID = platform::account_identifier_for_test(user3);
        let user3AccKey = open_market_account<FMA, FMB>(user3, vector[user3AccID]);
        deposit_to_market_account<FMA, FMB>(user3, user3AccKey, 1000000000000, 1000000000000);

        // Setup.
        add_user_limit_order<FMA, FMB>(user1, SIDE_BUY, BEHAVIOUR_GTC, 8000000000, 80000000000);
        let makerID = add_user_limit_order<FMA, FMB>(user2, SIDE_BUY, BEHAVIOUR_GTC, 9000000000, 70000000000);
        add_user_limit_order<FMA, FMB>(user1, SIDE_BUY, BEHAVIOUR_GTC, 9000000000, 60000000000);
        add_user_limit_order<FMA, FMB>(user1, SIDE_BUY, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user1, SIDE_BUY, BEHAVIOUR_GTC, 6000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user1, SIDE_BUY, BEHAVIOUR_GTC, 5000000000, 30000000000);
        let takerID = add_user_limit_order<FMA, FMB>(user3, SIDE_SELL, BEHAVIOUR_GTC, 6500000000, 30000000000);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.5 qty: 3, crankQty: 0)"),
            s(b"(0.6 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
            s(b"(0.8 qty: 8, crankQty: 0)"),
            s(b"(0.9 qty: 10, crankQty: 3)"),
        ]);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[], vector[]);
        assert_exec_events<FMA, FMB>(marketAddr, vector[
            ExecEventInfo {
                qty: 30000000000,
                takerOrderID: takerID,
                price: 9000000000,
            },
        ]);
        assert_order_qtys<FMA, FMB>(marketAddr, makerID, 70000000000, 0, 30000000000);
        assert_order_qtys<FMA, FMB>(marketAddr, takerID, 30000000000, 30000000000, 0);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert_order_collateral(book, takerID, 0, 30000000000);
        assert_order_collateral(book, makerID, 63000000000, 0);
        assert_order_used(book, takerID);
        assert_order_used(book, makerID);
        assert_account_balances(book, user2AccKey, 1000000000000, 937000000000);
        assert_account_balances(book, user3AccKey, 970000000000, 1000000000000);

        // Crank.
        crank<FMA, FMB>(user1, 1);
        assert_exec_events<FMA, FMB>(marketAddr, vector[]);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.5 qty: 3, crankQty: 0)"),
            s(b"(0.6 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
            s(b"(0.8 qty: 8, crankQty: 0)"),
            s(b"(0.9 qty: 10, crankQty: 0)"),
        ]);
        assert_order_qtys<FMA, FMB>(marketAddr, makerID, 40000000000, 0, 0);
        assert_order_qtys<FMA, FMB>(marketAddr, takerID, 0, 0, 0); // Taker is no longer used.
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert_order_collateral(book, takerID, 0, 0);
        assert_order_collateral(book, makerID, 36000000000, 0);
        assert_order_unused(book, takerID);
        assert_order_used(book, makerID);
        assert_account_balances(book, user2AccKey, 1030000000000, 937000000000);
        assert_account_balances(book, user3AccKey, 970000000000, 1027000000000);
    }

    #[test(aptos=@0x1, ferum=@ferum, user1=@0x3, user2=@0x4, user3=@0x5)]
    fun test_market_crank_qty_in_tree_buy(aptos: &signer, ferum: &signer, user1: &signer, user2: &signer, user3: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that an execution event for qty in the cache is processed correctly.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user1, 2);
        account::create_account_for_test(address_of(user2));
        deposit_fake_coins(ferum, 10000000000, user2);
        account::create_account_for_test(address_of(user3));
        deposit_fake_coins(ferum, 10000000000, user3);
        let user2AccID = platform::account_identifier_for_test(user2);
        let user2AccKey = open_market_account<FMA, FMB>(user2, vector[user2AccID]);
        deposit_to_market_account<FMA, FMB>(user2, user2AccKey, 1000000000000, 1000000000000);
        let user3AccID = platform::account_identifier_for_test(user3);
        let user3AccKey = open_market_account<FMA, FMB>(user3, vector[user3AccID]);
        deposit_to_market_account<FMA, FMB>(user3, user3AccKey, 1000000000000, 1000000000000);

        // Setup.
        let order1 = add_user_limit_order<FMA, FMB>(user1, SIDE_BUY, BEHAVIOUR_GTC, 8000000000, 80000000000);
        let order2 = add_user_limit_order<FMA, FMB>(user1, SIDE_BUY, BEHAVIOUR_GTC, 9000000000, 70000000000);
        let makerID = add_user_limit_order<FMA, FMB>(user2, SIDE_BUY, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user1, SIDE_BUY, BEHAVIOUR_GTC, 6000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user1, SIDE_BUY, BEHAVIOUR_GTC, 5000000000, 30000000000);
        cancel_order<FMA, FMB>(user1, order1);
        cancel_order<FMA, FMB>(user1, order2);
        let takerID = add_user_limit_order<FMA, FMB>(user3, SIDE_SELL, BEHAVIOUR_GTC, 6500000000, 30000000000);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.5 qty: 3, crankQty: 0)"),
            s(b"(0.6 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 2, crankQty: 3)"),
        ], vector[]);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[], vector[]);
        assert_exec_events<FMA, FMB>(marketAddr, vector[
            ExecEventInfo {
                qty: 30000000000,
                takerOrderID: takerID,
                price: 7000000000,
            },
        ]);
        assert_order_qtys<FMA, FMB>(marketAddr, makerID, 50000000000, 0, 30000000000);
        assert_order_qtys<FMA, FMB>(marketAddr, takerID, 30000000000, 30000000000, 0);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert_order_collateral(book, takerID, 0, 30000000000);
        assert_order_collateral(book, makerID, 35000000000, 0);
        assert_order_used(book, takerID);
        assert_order_used(book, makerID);
        assert_account_balances(book, user2AccKey, 1000000000000, 965000000000);
        assert_account_balances(book, user3AccKey, 970000000000, 1000000000000);

        // Crank.
        crank<FMA, FMB>(user1, 1);
        assert_exec_events<FMA, FMB>(marketAddr, vector[]);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.5 qty: 3, crankQty: 0)"),
            s(b"(0.6 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 2, crankQty: 0)"),
        ], vector[]);
        assert_order_qtys<FMA, FMB>(marketAddr, makerID, 20000000000, 0, 0);
        assert_order_qtys<FMA, FMB>(marketAddr, takerID, 0, 0, 0); // Taker is no longer used.
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert_order_collateral(book, takerID, 0, 0);
        assert_order_collateral(book, makerID, 14000000000, 0);
        assert_order_unused(book, takerID);
        assert_order_used(book, makerID);
        assert_account_balances(book, user2AccKey, 1030000000000, 965000000000);
        assert_account_balances(book, user3AccKey, 970000000000, 1021000000000);
    }

    #[test(aptos=@0x1, ferum=@ferum, user1=@0x3, user2=@0x4, user3=@0x5)]
    fun test_market_crank_qty_in_cache_remove_price_buy(aptos: &signer, ferum: &signer, user1: &signer, user2: &signer, user3: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that an execution event for qty in the cache removes the price level correctly.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user1, 2);
        account::create_account_for_test(address_of(user2));
        deposit_fake_coins(ferum, 10000000000, user2);
        account::create_account_for_test(address_of(user3));
        deposit_fake_coins(ferum, 10000000000, user3);
        let user2AccID = platform::account_identifier_for_test(user2);
        let user2AccKey = open_market_account<FMA, FMB>(user2, vector[user2AccID]);
        deposit_to_market_account<FMA, FMB>(user2, user2AccKey, 1000000000000, 1000000000000);
        let user3AccID = platform::account_identifier_for_test(user3);
        let user3AccKey = open_market_account<FMA, FMB>(user3, vector[user3AccID]);
        deposit_to_market_account<FMA, FMB>(user3, user3AccKey, 1000000000000, 1000000000000);

        // Setup.
        add_user_limit_order<FMA, FMB>(user1, SIDE_BUY, BEHAVIOUR_GTC, 8000000000, 80000000000);
        let makerID = add_user_limit_order<FMA, FMB>(user2, SIDE_BUY, BEHAVIOUR_GTC, 9000000000, 70000000000);
        add_user_limit_order<FMA, FMB>(user1, SIDE_BUY, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user1, SIDE_BUY, BEHAVIOUR_GTC, 6000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user1, SIDE_BUY, BEHAVIOUR_GTC, 5000000000, 30000000000);
        let takerID = add_user_limit_order<FMA, FMB>(user3, SIDE_SELL, BEHAVIOUR_GTC, 6500000000, 70000000000);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.5 qty: 3, crankQty: 0)"),
            s(b"(0.6 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
            s(b"(0.8 qty: 8, crankQty: 0)"),
            s(b"(0.9 qty: 0, crankQty: 7)"),
        ]);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[], vector[]);
        assert_exec_events<FMA, FMB>(marketAddr, vector[
            ExecEventInfo {
                qty: 70000000000,
                takerOrderID: takerID,
                price: 9000000000,
            },
        ]);
        assert_order_qtys<FMA, FMB>(marketAddr, makerID, 70000000000, 0, 70000000000);
        assert_order_qtys<FMA, FMB>(marketAddr, takerID, 70000000000, 70000000000, 0);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert_order_collateral(book, takerID, 0, 70000000000);
        assert_order_collateral(book, makerID, 63000000000, 0);
        assert_order_used(book, takerID);
        assert_order_used(book, makerID);
        assert_account_balances(book, user2AccKey, 1000000000000, 937000000000);
        assert_account_balances(book, user3AccKey, 930000000000, 1000000000000);
        assert!(book.summary.buyCacheMax == 9000000000, 0);
        assert!(book.summary.buyCacheMin == 8000000000, 0);
        assert!(book.summary.buyCacheQty == 80000000000, 0);
        assert!(book.summary.buyCacheSize == 2, 0);
        assert!(book.summary.buyTreeMax == 7000000000, 0);

        // Crank.
        crank<FMA, FMB>(user1, 1);
        assert_exec_events<FMA, FMB>(marketAddr, vector[]);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.5 qty: 3, crankQty: 0)"),
            s(b"(0.6 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
            s(b"(0.8 qty: 8, crankQty: 0)"),
        ]);
        assert_order_qtys<FMA, FMB>(marketAddr, makerID, 0, 0, 0); // Maker is no longer used.
        assert_order_qtys<FMA, FMB>(marketAddr, takerID, 0, 0, 0); // Taker is no longer used.
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert_order_collateral(book, takerID, 0, 0);
        assert_order_collateral(book, makerID, 0, 0);
        assert_order_unused(book, takerID);
        assert_order_unused(book, makerID);
        assert_account_balances(book, user2AccKey, 1070000000000, 937000000000);
        assert_account_balances(book, user3AccKey, 930000000000, 1063000000000);
        assert!(book.summary.buyCacheMax == 8000000000, 0);
        assert!(book.summary.buyCacheMin == 8000000000, 0);
        assert!(book.summary.buyCacheQty == 80000000000, 0);
        assert!(book.summary.buyCacheSize == 1, 0);
        assert!(book.summary.buyTreeMax == 7000000000, 0);
    }

    #[test(aptos=@0x1, ferum=@ferum, user1=@0x3, user2=@0x4, user3=@0x5)]
    fun test_market_crank_qty_in_tree_remove_price_buy(aptos: &signer, ferum: &signer, user1: &signer, user2: &signer, user3: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that an execution event for qty in the tree removes the price level correctly.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user1, 2);
        account::create_account_for_test(address_of(user2));
        deposit_fake_coins(ferum, 10000000000, user2);
        account::create_account_for_test(address_of(user3));
        deposit_fake_coins(ferum, 10000000000, user3);
        let user2AccID = platform::account_identifier_for_test(user2);
        let user2AccKey = open_market_account<FMA, FMB>(user2, vector[user2AccID]);
        deposit_to_market_account<FMA, FMB>(user2, user2AccKey, 1000000000000, 1000000000000);
        let user3AccID = platform::account_identifier_for_test(user3);
        let user3AccKey = open_market_account<FMA, FMB>(user3, vector[user3AccID]);
        deposit_to_market_account<FMA, FMB>(user3, user3AccKey, 1000000000000, 1000000000000);

        // Setup.
        let orderID2 = add_user_limit_order<FMA, FMB>(user1, SIDE_BUY, BEHAVIOUR_GTC, 8000000000, 80000000000);
        let orderID1 = add_user_limit_order<FMA, FMB>(user1, SIDE_BUY, BEHAVIOUR_GTC, 9000000000, 70000000000);
        let makerID1 = add_user_limit_order<FMA, FMB>(user2, SIDE_BUY, BEHAVIOUR_GTC, 7000000000, 50000000000);
        let makerID2 = add_user_limit_order<FMA, FMB>(user2, SIDE_BUY, BEHAVIOUR_GTC, 6000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user1, SIDE_BUY, BEHAVIOUR_GTC, 5000000000, 30000000000);
        cancel_order<FMA, FMB>(user1, orderID2);
        cancel_order<FMA, FMB>(user1, orderID1);
        let takerID = add_user_limit_order<FMA, FMB>(user3, SIDE_SELL, BEHAVIOUR_GTC, 5500000000, 70000000000);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.5 qty: 3, crankQty: 0)"),
            s(b"(0.6 qty: 2, crankQty: 2)"),
            s(b"(0.7 qty: 0, crankQty: 5)"),
        ], vector[]);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[], vector[]);
        assert_exec_events<FMA, FMB>(marketAddr, vector[
            ExecEventInfo {
                qty: 50000000000,
                takerOrderID: takerID,
                price: 7000000000,
            },
            ExecEventInfo {
                qty: 20000000000,
                takerOrderID: takerID,
                price: 6000000000,
            },
        ]);
        assert_order_qtys<FMA, FMB>(marketAddr, makerID1, 50000000000, 0, 50000000000);
        assert_order_qtys<FMA, FMB>(marketAddr, makerID2, 40000000000, 0, 20000000000);
        assert_order_qtys<FMA, FMB>(marketAddr, takerID, 70000000000, 70000000000, 0);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert_order_collateral(book, takerID, 0, 70000000000);
        assert_order_collateral(book, makerID1, 35000000000, 0);
        assert_order_collateral(book, makerID2, 24000000000, 0);
        assert_order_used(book, takerID);
        assert_order_used(book, makerID1);
        assert_order_used(book, makerID2);
        assert_account_balances(book, user2AccKey, 1000000000000, 941000000000);
        assert_account_balances(book, user3AccKey, 930000000000, 1000000000000);
        assert!(book.summary.buyCacheMax == 0, 0);
        assert!(book.summary.buyCacheMin == 0, 0);
        assert!(book.summary.buyCacheQty == 0, 0);
        assert!(book.summary.buyCacheSize == 0, 0);
        assert!(book.summary.buyTreeMax == 7000000000, 0);

        // Crank.
        crank<FMA, FMB>(user1, 1);
        assert_exec_events<FMA, FMB>(marketAddr, vector[]);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.5 qty: 3, crankQty: 0)"),
            s(b"(0.6 qty: 2, crankQty: 0)"),
        ], vector[]);
        assert_order_qtys<FMA, FMB>(marketAddr, makerID1, 0, 0, 0); // Maker 1 is no longer used.
        assert_order_qtys<FMA, FMB>(marketAddr, makerID2, 20000000000, 0, 0);
        assert_order_qtys<FMA, FMB>(marketAddr, takerID, 0, 0, 0); // Taker is no longer used.
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert_order_collateral(book, takerID, 0, 0);
        assert_order_collateral(book, makerID1, 0, 0);
        assert_order_collateral(book, makerID2, 12000000000, 0);
        assert_order_unused(book, takerID);
        assert_order_unused(book, makerID1);
        assert_order_used(book, makerID2);
        assert_account_balances(book, user2AccKey, 1070000000000, 941000000000);
        assert_account_balances(book, user3AccKey, 930000000000, 1047000000000);
        assert!(book.summary.buyCacheMax == 0, 0);
        assert!(book.summary.buyCacheMin == 0, 0);
        assert!(book.summary.buyCacheQty == 0, 0);
        assert!(book.summary.buyCacheSize == 0, 0);
        assert!(book.summary.buyTreeMax == 6000000000, 0);
    }

    #[test(aptos=@0x1, ferum=@ferum, user1=@0x3, makerUser=@0x4, takerUser=@0x6)]
    fun test_market_crank_not_finalize_taker_sell(
        aptos: &signer,
        ferum: &signer,
        user1: &signer,
        makerUser: &signer,
        takerUser: &signer,
    )
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that an execution event which executes a single maker partially is handled properly.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user1, 2);
        account::create_account_for_test(address_of(makerUser));
        deposit_fake_coins(ferum, 10000000000, makerUser);
        account::create_account_for_test(address_of(takerUser));
        deposit_fake_coins(ferum, 10000000000, takerUser);
        let makerAccID = platform::account_identifier_for_test(makerUser);
        let makerAccKey = open_market_account<FMA, FMB>(makerUser, vector[makerAccID]);
        deposit_to_market_account<FMA, FMB>(makerUser, makerAccKey, 1000000000000, 1000000000000);
        let takerAccID = platform::account_identifier_for_test(takerUser);
        let takerAccKey = open_market_account<FMA, FMB>(takerUser, vector[takerAccID]);
        deposit_to_market_account<FMA, FMB>(takerUser, takerAccKey, 1000000000000, 1000000000000);

        // Setup.
        add_user_limit_order<FMA, FMB>(user1, SIDE_SELL, BEHAVIOUR_GTC, 6000000000, 80000000000);
        let makerID = add_user_limit_order<FMA, FMB>(makerUser, SIDE_SELL, BEHAVIOUR_GTC, 5000000000, 70000000000);
        add_user_limit_order<FMA, FMB>(user1, SIDE_SELL, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user1, SIDE_SELL, BEHAVIOUR_GTC, 8000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user1, SIDE_SELL, BEHAVIOUR_GTC, 9000000000, 30000000000);
        let takerID = add_user_limit_order<FMA, FMB>(takerUser, SIDE_BUY, BEHAVIOUR_GTC, 5500000000, 80000000000);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.9 qty: 3, crankQty: 0)"),
            s(b"(0.8 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
            s(b"(0.6 qty: 8, crankQty: 0)"),
            s(b"(0.5 qty: 0, crankQty: 7)"),
        ]);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[], vector[
            s(b"(0.55 qty: 1, crankQty: 0)"),
        ]);
        assert_exec_events<FMA, FMB>(marketAddr, vector[
            ExecEventInfo {
                qty: 70000000000,
                takerOrderID: takerID,
                price: 5000000000,
            },
        ]);
        assert_order_qtys<FMA, FMB>(marketAddr, makerID, 70000000000, 0, 70000000000);
        assert_order_qtys<FMA, FMB>(marketAddr, takerID, 80000000000, 70000000000, 0);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert_order_collateral(book, takerID, 44000000000, 0);
        assert_order_collateral(book, makerID, 0, 70000000000);
        assert_order_used(book, takerID);
        assert_order_used(book, makerID);
        assert_account_balances(book, makerAccKey, 930000000000, 1000000000000);
        assert_account_balances(book, takerAccKey, 1000000000000, 956000000000);
        assert!(book.summary.sellCacheMax == 6000000000, 0);
        assert!(book.summary.sellCacheMin == 5000000000, 0);
        assert!(book.summary.sellCacheQty == 80000000000, 0);
        assert!(book.summary.sellCacheSize == 2, 0);
        assert!(book.summary.sellTreeMin == 7000000000, 0);
        assert!(book.summary.buyCacheMax == 5500000000, 0);
        assert!(book.summary.buyCacheMin == 5500000000, 0);
        assert!(book.summary.buyCacheQty == 10000000000, 0);
        assert!(book.summary.buyCacheSize == 1, 0);
        assert!(book.summary.buyTreeMax == 0, 0);

        // Crank.
        crank<FMA, FMB>(user1, 1);
        assert_exec_events<FMA, FMB>(marketAddr, vector[]);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.9 qty: 3, crankQty: 0)"),
            s(b"(0.8 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
            s(b"(0.6 qty: 8, crankQty: 0)"),
        ]);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[], vector[
            s(b"(0.55 qty: 1, crankQty: 0)"),
        ]);
        assert_order_qtys<FMA, FMB>(marketAddr, takerID, 10000000000, 0, 0);
        assert_order_qtys<FMA, FMB>(marketAddr, makerID, 0, 0, 0); // No longer used.
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert_order_collateral(book, makerID, 0, 0);
        assert_order_collateral(book, takerID, 5500000000, 0);
        assert_order_used(book, takerID);
        assert_order_unused(book, makerID);
        assert_account_balances(book, makerAccKey, 930000000000, 1035000000000);
        assert_account_balances(book, takerAccKey, 1070000000000, 959500000000);
        assert!(book.summary.sellCacheMax == 6000000000, 0);
        assert!(book.summary.sellCacheMin == 6000000000, 0);
        assert!(book.summary.sellCacheQty == 80000000000, 0);
        assert!(book.summary.sellCacheSize == 1, 0);
        assert!(book.summary.sellTreeMin == 7000000000, 0);
        assert!(book.summary.buyCacheMax == 5500000000, 0);
        assert!(book.summary.buyCacheMin == 5500000000, 0);
        assert!(book.summary.buyCacheQty == 10000000000, 0);
        assert!(book.summary.buyCacheSize == 1, 0);
        assert!(book.summary.buyTreeMax == 0, 0);
    }

    #[test(aptos=@0x1, ferum=@ferum, user1=@0x3, makerUser1=@0x4, makerUser2=@0x5, takerUser=@0x6)]
    fun test_market_crank_execute_against_multiple_makers_sell(
        aptos: &signer,
        ferum: &signer,
        user1: &signer,
        makerUser1: &signer,
        makerUser2: &signer,
        takerUser: &signer,
    )
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that an execution event which execute against multiple makers is processed correctly.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user1, 2);
        account::create_account_for_test(address_of(makerUser1));
        deposit_fake_coins(ferum, 10000000000, makerUser1);
        account::create_account_for_test(address_of(makerUser2));
        deposit_fake_coins(ferum, 10000000000, makerUser2);
        account::create_account_for_test(address_of(takerUser));
        deposit_fake_coins(ferum, 10000000000, takerUser);
        let maker1AccID = platform::account_identifier_for_test(makerUser1);
        let maker1AccKey = open_market_account<FMA, FMB>(makerUser1, vector[maker1AccID]);
        deposit_to_market_account<FMA, FMB>(makerUser1, maker1AccKey, 1000000000000, 1000000000000);
        let maker2AccID = platform::account_identifier_for_test(makerUser2);
        let maker2AccKey = open_market_account<FMA, FMB>(makerUser2, vector[maker2AccID]);
        deposit_to_market_account<FMA, FMB>(makerUser2, maker2AccKey, 1000000000000, 1000000000000);
        let takerAccID = platform::account_identifier_for_test(takerUser);
        let takerAccKey = open_market_account<FMA, FMB>(takerUser, vector[takerAccID]);
        deposit_to_market_account<FMA, FMB>(takerUser, takerAccKey, 1000000000000, 1000000000000);

        // Setup.
        let makerID3 = add_user_limit_order<FMA, FMB>(makerUser2, SIDE_SELL, BEHAVIOUR_GTC, 6000000000, 80000000000);
        let makerID1 = add_user_limit_order<FMA, FMB>(makerUser1, SIDE_SELL, BEHAVIOUR_GTC, 5000000000, 70000000000);
        let makerID2 = add_user_limit_order<FMA, FMB>(makerUser1, SIDE_SELL, BEHAVIOUR_GTC, 5000000000, 60000000000);
        let makerID4 = add_user_limit_order<FMA, FMB>(makerUser2, SIDE_SELL, BEHAVIOUR_GTC, 7000000000, 50000000000);
        let makerID5 = add_user_limit_order<FMA, FMB>(makerUser2, SIDE_SELL, BEHAVIOUR_GTC, 8000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user1, SIDE_SELL, BEHAVIOUR_GTC, 9000000000, 30000000000);
        let takerID = add_user_limit_order<FMA, FMB>(takerUser, SIDE_BUY, BEHAVIOUR_GTC, 8500000000, 300000000000);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.9 qty: 3, crankQty: 0)"),
            s(b"(0.8 qty: 0, crankQty: 4)"),
            s(b"(0.7 qty: 0, crankQty: 5)"),
        ], vector[
            s(b"(0.6 qty: 0, crankQty: 8)"),
            s(b"(0.5 qty: 0, crankQty: 13)"),
        ]);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[], vector[]);
        assert_exec_events<FMA, FMB>(marketAddr, vector[
            ExecEventInfo {
                qty: 130000000000,
                takerOrderID: takerID,
                price: 5000000000,
            },
            ExecEventInfo {
                qty: 80000000000,
                takerOrderID: takerID,
                price: 6000000000,
            },
            ExecEventInfo {
                qty: 50000000000,
                takerOrderID: takerID,
                price: 7000000000,
            },
            ExecEventInfo {
                qty: 40000000000,
                takerOrderID: takerID,
                price: 8000000000,
            },
        ]);
        assert_order_qtys<FMA, FMB>(marketAddr, makerID1, 70000000000, 0, 70000000000);
        assert_order_qtys<FMA, FMB>(marketAddr, makerID2, 60000000000, 0, 60000000000);
        assert_order_qtys<FMA, FMB>(marketAddr, makerID3, 80000000000, 0, 80000000000);
        assert_order_qtys<FMA, FMB>(marketAddr, makerID4, 50000000000, 0, 50000000000);
        assert_order_qtys<FMA, FMB>(marketAddr, makerID5, 40000000000, 0, 40000000000);
        assert_order_qtys<FMA, FMB>(marketAddr, takerID, 300000000000, 300000000000, 0);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert_order_collateral(book, takerID, 255000000000, 0);
        assert_order_collateral(book, makerID1, 0, 70000000000);
        assert_order_collateral(book, makerID2, 0, 60000000000);
        assert_order_collateral(book, makerID3, 0, 80000000000);
        assert_order_collateral(book, makerID4, 0, 50000000000);
        assert_order_collateral(book, makerID5, 0, 40000000000);
        assert_order_used(book, takerID);
        assert_order_used(book, makerID1);
        assert_order_used(book, makerID2);
        assert_order_used(book, makerID3);
        assert_order_used(book, makerID4);
        assert_order_used(book, makerID5);
        assert_account_balances(book, maker1AccKey, 870000000000, 1000000000000);
        assert_account_balances(book, maker2AccKey, 830000000000, 1000000000000);
        assert_account_balances(book, takerAccKey, 1000000000000, 745000000000);
        assert!(book.summary.sellCacheMax == 6000000000, 0);
        assert!(book.summary.sellCacheMin == 5000000000, 0);
        assert!(book.summary.sellCacheQty == 0, 0);
        assert!(book.summary.sellCacheSize == 2, 0);
        assert!(book.summary.sellTreeMin == 7000000000, 0);

        // Crank.
        crank<FMA, FMB>(user1, 1);
        assert_exec_events<FMA, FMB>(marketAddr, vector[]);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.9 qty: 3, crankQty: 0)"),
        ], vector[]);
        assert_order_qtys<FMA, FMB>(marketAddr, makerID1, 0, 0, 0); // No longer used.
        assert_order_qtys<FMA, FMB>(marketAddr, makerID2, 0, 0, 0); // No longer used.
        assert_order_qtys<FMA, FMB>(marketAddr, makerID3, 0, 0, 0); // No longer used.
        assert_order_qtys<FMA, FMB>(marketAddr, makerID4, 0, 0, 0); // No longer used.
        assert_order_qtys<FMA, FMB>(marketAddr, makerID5, 0, 0, 0); // No longer used.
        assert_order_qtys<FMA, FMB>(marketAddr, takerID, 0, 0, 0); // No longer used.
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert_order_collateral(book, takerID, 0, 0);
        assert_order_collateral(book, makerID1, 0, 0);
        assert_order_collateral(book, makerID2, 0, 0);
        assert_order_collateral(book, makerID3, 0, 0);
        assert_order_collateral(book, makerID4, 0, 0);
        assert_order_collateral(book, makerID5, 0, 0);
        assert_order_unused(book, makerID1);
        assert_order_unused(book, makerID2);
        assert_order_unused(book, makerID3);
        assert_order_unused(book, makerID4);
        assert_order_unused(book, makerID5);
        assert_order_unused(book, takerID);
        assert_account_balances(book, maker1AccKey, 870000000000, 1065000000000);
        assert_account_balances(book, maker2AccKey, 830000000000, 1115000000000);
        assert_account_balances(book, takerAccKey, 1300000000000, 820000000000);
        assert!(book.summary.sellCacheMax == 0, 0);
        assert!(book.summary.sellCacheMin == 0, 0);
        assert!(book.summary.sellCacheQty == 0, 0);
        assert!(book.summary.sellCacheSize == 0, 0);
        assert!(book.summary.sellTreeMin == 9000000000, 0);
    }

    #[test(aptos=@0x1, ferum=@ferum, user1=@0x3, user2=@0x4, user3=@0x5)]
    fun test_market_crank_qty_in_cache_sell(aptos: &signer, ferum: &signer, user1: &signer, user2: &signer, user3: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that an execution event for qty in the cache is processed correctly.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user1, 2);
        account::create_account_for_test(address_of(user2));
        deposit_fake_coins(ferum, 10000000000, user2);
        account::create_account_for_test(address_of(user3));
        deposit_fake_coins(ferum, 10000000000, user3);
        let user2AccID = platform::account_identifier_for_test(user2);
        let user2AccKey = open_market_account<FMA, FMB>(user2, vector[user2AccID]);
        deposit_to_market_account<FMA, FMB>(user2, user2AccKey, 1000000000000, 1000000000000);
        let user3AccID = platform::account_identifier_for_test(user3);
        let user3AccKey = open_market_account<FMA, FMB>(user3, vector[user3AccID]);
        deposit_to_market_account<FMA, FMB>(user3, user3AccKey, 1000000000000, 1000000000000);

        // Setup.
        add_user_limit_order<FMA, FMB>(user1, SIDE_SELL, BEHAVIOUR_GTC, 6000000000, 80000000000);
        let makerID = add_user_limit_order<FMA, FMB>(user2, SIDE_SELL, BEHAVIOUR_GTC, 5000000000, 70000000000);
        add_user_limit_order<FMA, FMB>(user1, SIDE_SELL, BEHAVIOUR_GTC, 5000000000, 60000000000);
        add_user_limit_order<FMA, FMB>(user1, SIDE_SELL, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user1, SIDE_SELL, BEHAVIOUR_GTC, 9000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user1, SIDE_SELL, BEHAVIOUR_GTC, 8000000000, 30000000000);
        let takerID = add_user_limit_order<FMA, FMB>(user3, SIDE_BUY, BEHAVIOUR_GTC, 5500000000, 30000000000);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.9 qty: 4, crankQty: 0)"),
            s(b"(0.8 qty: 3, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
            s(b"(0.6 qty: 8, crankQty: 0)"),
            s(b"(0.5 qty: 10, crankQty: 3)"),
        ]);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[], vector[]);
        assert_exec_events<FMA, FMB>(marketAddr, vector[
            ExecEventInfo {
                qty: 30000000000,
                takerOrderID: takerID,
                price: 5000000000,
            },
        ]);
        assert_order_qtys<FMA, FMB>(marketAddr, makerID, 70000000000, 0, 30000000000);
        assert_order_qtys<FMA, FMB>(marketAddr, takerID, 30000000000, 30000000000, 0);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert_order_collateral(book, takerID, 16500000000, 0);
        assert_order_collateral(book, makerID, 0, 70000000000);
        assert_order_used(book, takerID);
        assert_order_used(book, makerID);
        assert_account_balances(book, user2AccKey, 930000000000, 1000000000000);
        assert_account_balances(book, user3AccKey, 1000000000000, 983500000000);

        // Crank.
        crank<FMA, FMB>(user1, 1);
        assert_exec_events<FMA, FMB>(marketAddr, vector[]);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.9 qty: 4, crankQty: 0)"),
            s(b"(0.8 qty: 3, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
            s(b"(0.6 qty: 8, crankQty: 0)"),
            s(b"(0.5 qty: 10, crankQty: 0)"),
        ]);
        assert_order_qtys<FMA, FMB>(marketAddr, makerID, 40000000000, 0, 0);
        assert_order_qtys<FMA, FMB>(marketAddr, takerID, 0, 0, 0); // Taker is no longer used.
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert_order_collateral(book, takerID, 0, 0);
        assert_order_collateral(book, makerID, 0, 40000000000);
        assert_order_unused(book, takerID);
        assert_order_used(book, makerID);
        assert_account_balances(book, user2AccKey, 930000000000, 1015000000000);
        assert_account_balances(book, user3AccKey, 1030000000000, 985000000000);
    }

    #[test(aptos=@0x1, ferum=@ferum, user1=@0x3, user2=@0x4, user3=@0x5)]
    fun test_market_crank_qty_in_tree_sell(aptos: &signer, ferum: &signer, user1: &signer, user2: &signer, user3: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that an execution event for qty in the cache is processed correctly.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user1, 2);
        account::create_account_for_test(address_of(user2));
        deposit_fake_coins(ferum, 10000000000, user2);
        account::create_account_for_test(address_of(user3));
        deposit_fake_coins(ferum, 10000000000, user3);
        let user2AccID = platform::account_identifier_for_test(user2);
        let user2AccKey = open_market_account<FMA, FMB>(user2, vector[user2AccID]);
        deposit_to_market_account<FMA, FMB>(user2, user2AccKey, 1000000000000, 1000000000000);
        let user3AccID = platform::account_identifier_for_test(user3);
        let user3AccKey = open_market_account<FMA, FMB>(user3, vector[user3AccID]);
        deposit_to_market_account<FMA, FMB>(user3, user3AccKey, 1000000000000, 1000000000000);

        // Setup.
        let orderID1 = add_user_limit_order<FMA, FMB>(user1, SIDE_SELL, BEHAVIOUR_GTC, 6000000000, 80000000000);
        let orderID2 = add_user_limit_order<FMA, FMB>(user1, SIDE_SELL, BEHAVIOUR_GTC, 5000000000, 70000000000);
        let makerID = add_user_limit_order<FMA, FMB>(user2, SIDE_SELL, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user1, SIDE_SELL, BEHAVIOUR_GTC, 9000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user1, SIDE_SELL, BEHAVIOUR_GTC, 8000000000, 30000000000);
        cancel_order<FMA, FMB>(user1, orderID1);
        cancel_order<FMA, FMB>(user1, orderID2);
        let takerID = add_user_limit_order<FMA, FMB>(user3, SIDE_BUY, BEHAVIOUR_GTC, 8500000000, 30000000000);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.9 qty: 4, crankQty: 0)"),
            s(b"(0.8 qty: 3, crankQty: 0)"),
            s(b"(0.7 qty: 2, crankQty: 3)"),
        ], vector[]);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[], vector[]);
        assert_exec_events<FMA, FMB>(marketAddr, vector[
            ExecEventInfo {
                qty: 30000000000,
                takerOrderID: takerID,
                price: 7000000000,
            },
        ]);
        assert_order_qtys<FMA, FMB>(marketAddr, makerID, 50000000000, 0, 30000000000);
        assert_order_qtys<FMA, FMB>(marketAddr, takerID, 30000000000, 30000000000, 0);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert_order_collateral(book, takerID, 25500000000, 0);
        assert_order_collateral(book, makerID, 0, 50000000000);
        assert_order_used(book, takerID);
        assert_order_used(book, makerID);
        assert_account_balances(book, user2AccKey, 950000000000, 1000000000000);
        assert_account_balances(book, user3AccKey, 1000000000000, 974500000000);

        // Crank.
        crank<FMA, FMB>(user1, 1);
        assert_exec_events<FMA, FMB>(marketAddr, vector[]);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.9 qty: 4, crankQty: 0)"),
            s(b"(0.8 qty: 3, crankQty: 0)"),
            s(b"(0.7 qty: 2, crankQty: 0)"),
        ], vector[]);
        assert_order_qtys<FMA, FMB>(marketAddr, makerID, 20000000000, 0, 0);
        assert_order_qtys<FMA, FMB>(marketAddr, takerID, 0, 0, 0); // Taker is no longer used.
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert_order_collateral(book, takerID, 0, 0);
        assert_order_collateral(book, makerID, 0, 20000000000);
        assert_order_unused(book, takerID);
        assert_order_used(book, makerID);
        assert_account_balances(book, user2AccKey, 950000000000, 1021000000000);
        assert_account_balances(book, user3AccKey, 1030000000000, 979000000000);
    }

    #[test(aptos=@0x1, ferum=@ferum, user1=@0x3, user2=@0x4, user3=@0x5)]
    fun test_market_crank_qty_in_cache_remove_price_sell(aptos: &signer, ferum: &signer, user1: &signer, user2: &signer, user3: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that an execution event for qty in the cache removes the price level correctly.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user1, 2);
        account::create_account_for_test(address_of(user2));
        deposit_fake_coins(ferum, 10000000000, user2);
        account::create_account_for_test(address_of(user3));
        deposit_fake_coins(ferum, 10000000000, user3);
        let user2AccID = platform::account_identifier_for_test(user2);
        let user2AccKey = open_market_account<FMA, FMB>(user2, vector[user2AccID]);
        deposit_to_market_account<FMA, FMB>(user2, user2AccKey, 1000000000000, 1000000000000);
        let user3AccID = platform::account_identifier_for_test(user3);
        let user3AccKey = open_market_account<FMA, FMB>(user3, vector[user3AccID]);
        deposit_to_market_account<FMA, FMB>(user3, user3AccKey, 1000000000000, 1000000000000);

        // Setup.
        add_user_limit_order<FMA, FMB>(user1, SIDE_SELL, BEHAVIOUR_GTC, 6000000000, 80000000000);
        let makerID = add_user_limit_order<FMA, FMB>(user2, SIDE_SELL, BEHAVIOUR_GTC, 5000000000, 70000000000);
        add_user_limit_order<FMA, FMB>(user1, SIDE_SELL, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user1, SIDE_SELL, BEHAVIOUR_GTC, 9000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user1, SIDE_SELL, BEHAVIOUR_GTC, 8000000000, 30000000000);
        let takerID = add_user_limit_order<FMA, FMB>(user3, SIDE_BUY, BEHAVIOUR_GTC, 5500000000, 70000000000);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.9 qty: 4, crankQty: 0)"),
            s(b"(0.8 qty: 3, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
            s(b"(0.6 qty: 8, crankQty: 0)"),
            s(b"(0.5 qty: 0, crankQty: 7)"),
        ]);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[], vector[]);
        assert_exec_events<FMA, FMB>(marketAddr, vector[
            ExecEventInfo {
                qty: 70000000000,
                takerOrderID: takerID,
                price: 5000000000,
            },
        ]);
        assert_order_qtys<FMA, FMB>(marketAddr, makerID, 70000000000, 0, 70000000000);
        assert_order_qtys<FMA, FMB>(marketAddr, takerID, 70000000000, 70000000000, 0);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert_order_collateral(book, takerID, 38500000000, 0);
        assert_order_collateral(book, makerID, 0, 70000000000);
        assert_order_used(book, takerID);
        assert_order_used(book, makerID);
        assert_account_balances(book, user2AccKey, 930000000000, 1000000000000);
        assert_account_balances(book, user3AccKey, 1000000000000, 961500000000);
        assert!(book.summary.sellCacheMax == 6000000000, 0);
        assert!(book.summary.sellCacheMin == 5000000000, 0);
        assert!(book.summary.sellCacheQty == 80000000000, 0);
        assert!(book.summary.sellCacheSize == 2, 0);
        assert!(book.summary.sellTreeMin == 7000000000, 0);

        // Crank.
        crank<FMA, FMB>(user1, 1);
        assert_exec_events<FMA, FMB>(marketAddr, vector[]);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.9 qty: 4, crankQty: 0)"),
            s(b"(0.8 qty: 3, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
            s(b"(0.6 qty: 8, crankQty: 0)"),
        ]);
        assert_order_qtys<FMA, FMB>(marketAddr, makerID, 0, 0, 0); // Maker is no longer used.
        assert_order_qtys<FMA, FMB>(marketAddr, takerID, 0, 0, 0); // Taker is no longer used.
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert_order_collateral(book, takerID, 0, 0);
        assert_order_collateral(book, makerID, 0, 0);
        assert_order_unused(book, takerID);
        assert_order_unused(book, makerID);
        assert_account_balances(book, user2AccKey, 930000000000, 1035000000000);
        assert_account_balances(book, user3AccKey, 1070000000000, 965000000000);
        assert!(book.summary.sellCacheMax == 6000000000, 0);
        assert!(book.summary.sellCacheMin == 6000000000, 0);
        assert!(book.summary.sellCacheQty == 80000000000, 0);
        assert!(book.summary.sellCacheSize == 1, 0);
        assert!(book.summary.sellTreeMin == 7000000000, 0);
    }

    #[test(aptos=@0x1, ferum=@ferum, user1=@0x3, user2=@0x4, user3=@0x5)]
    fun test_market_crank_qty_in_tree_remove_price_sell(aptos: &signer, ferum: &signer, user1: &signer, user2: &signer, user3: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that an execution event for qty in the cache removes the price level correctly.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user1, 2);
        account::create_account_for_test(address_of(user2));
        deposit_fake_coins(ferum, 10000000000, user2);
        account::create_account_for_test(address_of(user3));
        deposit_fake_coins(ferum, 10000000000, user3);
        let user2AccID = platform::account_identifier_for_test(user2);
        let user2AccKey = open_market_account<FMA, FMB>(user2, vector[user2AccID]);
        deposit_to_market_account<FMA, FMB>(user2, user2AccKey, 1000000000000, 1000000000000);
        let user3AccID = platform::account_identifier_for_test(user3);
        let user3AccKey = open_market_account<FMA, FMB>(user3, vector[user3AccID]);
        deposit_to_market_account<FMA, FMB>(user3, user3AccKey, 1000000000000, 1000000000000);

        // Setup.
        let orderID1 = add_user_limit_order<FMA, FMB>(user1, SIDE_SELL, BEHAVIOUR_GTC, 6000000000, 80000000000);
        let orderID2 = add_user_limit_order<FMA, FMB>(user1, SIDE_SELL, BEHAVIOUR_GTC, 5000000000, 70000000000);
        let makerID1 = add_user_limit_order<FMA, FMB>(user2, SIDE_SELL, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user1, SIDE_SELL, BEHAVIOUR_GTC, 9000000000, 40000000000);
        let makerID2 = add_user_limit_order<FMA, FMB>(user2, SIDE_SELL, BEHAVIOUR_GTC, 8000000000, 30000000000);
        cancel_order<FMA, FMB>(user1, orderID1);
        cancel_order<FMA, FMB>(user1, orderID2);
        let takerID = add_user_limit_order<FMA, FMB>(user3, SIDE_BUY, BEHAVIOUR_GTC, 8500000000, 70000000000);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.9 qty: 4, crankQty: 0)"),
            s(b"(0.8 qty: 1, crankQty: 2)"),
            s(b"(0.7 qty: 0, crankQty: 5)"),
        ], vector[]);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[], vector[]);
        assert_exec_events<FMA, FMB>(marketAddr, vector[
            ExecEventInfo {
                qty: 50000000000,
                takerOrderID: takerID,
                price: 7000000000,
            },
            ExecEventInfo {
                qty: 20000000000,
                takerOrderID: takerID,
                price: 8000000000,
            },
        ]);
        assert_order_qtys<FMA, FMB>(marketAddr, makerID1, 50000000000, 0, 50000000000);
        assert_order_qtys<FMA, FMB>(marketAddr, makerID2, 30000000000, 0, 20000000000);
        assert_order_qtys<FMA, FMB>(marketAddr, takerID, 70000000000, 70000000000, 0);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert_order_collateral(book, takerID, 59500000000, 0);
        assert_order_collateral(book, makerID1, 0, 50000000000);
        assert_order_collateral(book, makerID2, 0, 30000000000);
        assert_order_used(book, takerID);
        assert_order_used(book, makerID1);
        assert_order_used(book, makerID2);
        assert_account_balances(book, user2AccKey, 920000000000, 1000000000000);
        assert_account_balances(book, user3AccKey, 1000000000000, 940500000000);
        assert!(book.summary.sellCacheMax == 0, 0);
        assert!(book.summary.sellCacheMin == 0, 0);
        assert!(book.summary.sellCacheQty == 0, 0);
        assert!(book.summary.sellCacheSize == 0, 0);
        assert!(book.summary.sellTreeMin == 7000000000, 0);

        // Crank.
        crank<FMA, FMB>(user1, 1);
        assert_exec_events<FMA, FMB>(marketAddr, vector[]);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.9 qty: 4, crankQty: 0)"),
            s(b"(0.8 qty: 1, crankQty: 0)"),
        ], vector[]);
        assert_order_qtys<FMA, FMB>(marketAddr, makerID1, 0, 0, 0); // Maker 1 is no longer used.
        assert_order_qtys<FMA, FMB>(marketAddr, makerID2, 10000000000, 0, 0);
        assert_order_qtys<FMA, FMB>(marketAddr, takerID, 0, 0, 0); // Taker is no longer used.
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert_order_collateral(book, takerID, 0, 0);
        assert_order_collateral(book, makerID1, 0, 0);
        assert_order_collateral(book, makerID2, 0, 10000000000);
        assert_order_unused(book, takerID);
        assert_order_unused(book, makerID1);
        assert_order_used(book, makerID2);
        assert_account_balances(book, user2AccKey, 920000000000, 1051000000000);
        assert_account_balances(book, user3AccKey, 1070000000000, 949000000000);
        assert!(book.summary.sellCacheMax == 0, 0);
        assert!(book.summary.sellCacheMin == 0, 0);
        assert!(book.summary.sellCacheQty == 0, 0);
        assert!(book.summary.sellCacheSize == 0, 0);
        assert!(book.summary.sellTreeMin == 8000000000, 0);
    }

    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="Price store insert/remove tests">

    // <editor-fold defaultstate="collapsed" desc="Buy side">

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    fun test_market_price_store_cache_overflow_buy(aptos: &signer, ferum: &signer, user: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that added buy prices are put into the tree when the cache is full and the order falls outside the
        // range of the cache. If the cache is full but orders fall into the cache range, it is placed into the cache.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 4);

        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 11234000000, 10000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC,  8000000000, 10240000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC,  9000000000, 11240000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC,  9000000000, 11240000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC,  7000000000, 11240000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC,  7500000000, 11250000000);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert_price_levels(book, vector[
            s(b"BUY 0.7[4]: (5, 1.124)"),
            s(b"BUY 0.75[5]: (6, 1.125)"),
            s(b"BUY 0.8[2]: (2, 1.024)"),
            s(b"BUY 0.9[3]: (3, 1.124) (4, 1.124)"),
            s(b"BUY 1.1234[1]: (1, 1)"),
        ]);
        let tree = &borrow_global<MarketBuyTree<FMA, FMB>>(marketAddr).tree;
        assert_market_tree(tree, SIDE_BUY, vector[]);
        let cache = &borrow_global<MarketBuyCache<FMA, FMB>>(marketAddr).cache;
        assert_market_cache(cache, vector[
            s(b"(0.7 priceLevelID: 4, qty: 1.124, crankQty: 0)"),
            s(b"(0.75 priceLevelID: 5, qty: 1.125, crankQty: 0)"),
            s(b"(0.8 priceLevelID: 2, qty: 1.024, crankQty: 0)"),
            s(b"(0.9 priceLevelID: 3, qty: 2.248, crankQty: 0)"),
            s(b"(1.1234 priceLevelID: 1, qty: 1, crankQty: 0)")
        ]);
        assert!(book.summary.buyCacheMax == 11234000000, 0);
        assert!(book.summary.buyCacheMin == 7000000000, 0);
        assert!(book.summary.buyCacheQty == 65210000000, 0);
        assert!(book.summary.buyCacheSize == 5, 0);
        assert!(book.summary.buyTreeMax == 0, 0);

        // These orders should be inserted into the tree.
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC,  6000000000, 11240000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC,  6500000000, 11340000000);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert_price_levels(book, vector[
            s(b"BUY 0.6[6]: (7, 1.124)"),
            s(b"BUY 0.65[7]: (8, 1.134)"),
            s(b"BUY 0.7[4]: (5, 1.124)"),
            s(b"BUY 0.75[5]: (6, 1.125)"),
            s(b"BUY 0.8[2]: (2, 1.024)"),
            s(b"BUY 0.9[3]: (3, 1.124) (4, 1.124)"),
            s(b"BUY 1.1234[1]: (1, 1)"),
        ]);
        let tree = &borrow_global<MarketBuyTree<FMA, FMB>>(marketAddr).tree;
        assert_market_tree(tree, SIDE_BUY, vector[
            s(b"(0.6 priceLevelID: 6, qty: 1.124, crankQty: 0)"),
            s(b"(0.65 priceLevelID: 7, qty: 1.134, crankQty: 0)"),
        ]);
        let cache = &borrow_global<MarketBuyCache<FMA, FMB>>(marketAddr).cache;
        assert_market_cache(cache, vector[
            s(b"(0.7 priceLevelID: 4, qty: 1.124, crankQty: 0)"),
            s(b"(0.75 priceLevelID: 5, qty: 1.125, crankQty: 0)"),
            s(b"(0.8 priceLevelID: 2, qty: 1.024, crankQty: 0)"),
            s(b"(0.9 priceLevelID: 3, qty: 2.248, crankQty: 0)"),
            s(b"(1.1234 priceLevelID: 1, qty: 1, crankQty: 0)")
        ]);
        assert!(book.summary.buyCacheMax == 11234000000, 0);
        assert!(book.summary.buyCacheMin == 7000000000, 0);
        assert!(book.summary.buyCacheQty == 65210000000, 0);
        assert!(book.summary.buyCacheSize == 5, 0);
        assert!(book.summary.buyTreeMax == 6500000000, 0);

        // These orders should be inserted into the cache because they fall into the range of the cache.
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC,  10000000000, 11240000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC,  15000000000, 11340000000);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert_price_levels(book, vector[
            s(b"BUY 0.6[6]: (7, 1.124)"),
            s(b"BUY 0.65[7]: (8, 1.134)"),
            s(b"BUY 0.7[4]: (5, 1.124)"),
            s(b"BUY 0.75[5]: (6, 1.125)"),
            s(b"BUY 0.8[2]: (2, 1.024)"),
            s(b"BUY 0.9[3]: (3, 1.124) (4, 1.124)"),
            s(b"BUY 1[8]: (9, 1.124)"),
            s(b"BUY 1.1234[1]: (1, 1)"),
            s(b"BUY 1.5[9]: (10, 1.134)"),
        ]);
        let tree = &borrow_global<MarketBuyTree<FMA, FMB>>(marketAddr).tree;
        assert_market_tree(tree, SIDE_BUY, vector[
            s(b"(0.6 priceLevelID: 6, qty: 1.124, crankQty: 0)"),
            s(b"(0.65 priceLevelID: 7, qty: 1.134, crankQty: 0)"),
        ]);
        let cache = &borrow_global<MarketBuyCache<FMA, FMB>>(marketAddr).cache;
        assert_market_cache(cache, vector[
            s(b"(0.7 priceLevelID: 4, qty: 1.124, crankQty: 0)"),
            s(b"(0.75 priceLevelID: 5, qty: 1.125, crankQty: 0)"),
            s(b"(0.8 priceLevelID: 2, qty: 1.024, crankQty: 0)"),
            s(b"(0.9 priceLevelID: 3, qty: 2.248, crankQty: 0)"),
            s(b"(1 priceLevelID: 8, qty: 1.124, crankQty: 0)"),
            s(b"(1.1234 priceLevelID: 1, qty: 1, crankQty: 0)"),
            s(b"(1.5 priceLevelID: 9, qty: 1.134, crankQty: 0)"),
        ]);
        assert!(book.summary.buyCacheMax == 15000000000, 0);
        assert!(book.summary.buyCacheMin == 7000000000, 0);
        assert!(book.summary.buyCacheQty == 87790000000, 0);
        assert!(book.summary.buyCacheSize == 7, 0);
        assert!(book.summary.buyTreeMax == 6500000000, 0);
    }

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    fun test_market_price_store_remove_cache_underflow_buy(aptos: &signer, ferum: &signer, user: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that a removed buy price levels doesn't cause prices to move from the cache to the tree.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 4);

        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 8000000000, 80000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 9000000000, 70000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 9000000000, 60000000000);
        let orderID = add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 6000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 5000000000, 30000000000);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert_price_levels(book, vector[
            s(b"BUY 0.5[5]: (6, 3)"),
            s(b"BUY 0.6[4]: (5, 4)"),
            s(b"BUY 0.7[3]: (4, 5)"),
            s(b"BUY 0.8[1]: (1, 8)"),
            s(b"BUY 0.9[2]: (2, 7) (3, 6)"),
        ]);
        let tree = &borrow_global<MarketBuyTree<FMA, FMB>>(marketAddr).tree;
        assert_market_tree(tree, SIDE_BUY, vector[
            s(b"(0.5 priceLevelID: 5, qty: 3, crankQty: 0)"),
        ]);
        let cache = &borrow_global<MarketBuyCache<FMA, FMB>>(marketAddr).cache;
        assert_market_cache(cache, vector[
            s(b"(0.6 priceLevelID: 4, qty: 4, crankQty: 0)"),
            s(b"(0.7 priceLevelID: 3, qty: 5, crankQty: 0)"),
            s(b"(0.8 priceLevelID: 1, qty: 8, crankQty: 0)"),
            s(b"(0.9 priceLevelID: 2, qty: 13, crankQty: 0)"),
        ]);

        // Remove price level from cache by cancelling an order.
        cancel_order<FMA, FMB>(user, orderID);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert_price_levels(book, vector[
            s(b"BUY 0.5[5]: (6, 3)"),
            s(b"BUY 0.6[4]: (5, 4)"),
            s(b"BUY 0.8[1]: (1, 8)"),
            s(b"BUY 0.9[2]: (2, 7) (3, 6)"),
        ]);
        let tree = &borrow_global<MarketBuyTree<FMA, FMB>>(marketAddr).tree;
        assert_market_tree(tree, SIDE_BUY, vector[
            s(b"(0.5 priceLevelID: 5, qty: 3, crankQty: 0)"),
        ]);
        let cache = &borrow_global<MarketBuyCache<FMA, FMB>>(marketAddr).cache;
        assert_market_cache(cache, vector[
            s(b"(0.6 priceLevelID: 4, qty: 4, crankQty: 0)"),
            s(b"(0.8 priceLevelID: 1, qty: 8, crankQty: 0)"),
            s(b"(0.9 priceLevelID: 2, qty: 13, crankQty: 0)"),
        ]);
        assert!(book.summary.buyCacheMax == 9000000000, 0);
        assert!(book.summary.buyCacheMin == 6000000000, 0);
        assert!(book.summary.buyCacheQty == 250000000000, 0);
        assert!(book.summary.buyCacheSize == 3, 0);
        assert!(book.summary.buyTreeMax == 5000000000, 0);
    }

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    fun test_market_price_store_cache_underflow_add_to_tree_buy(aptos: &signer, ferum: &signer, user: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that a price is added to the tree if it falls into the range defined by the tree, even if the cache
        // has space.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 4);

        // First insert into both cache and tree, then remove so that there are prices in the tree and in the cache.
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 8000000000, 80000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 9000000000, 70000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 9000000000, 60000000000);
        let orderID = add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 6000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 5000000000, 30000000000);
        cancel_order<FMA, FMB>(user, orderID);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert_price_levels(book, vector[
            s(b"BUY 0.5[5]: (6, 3)"),
            s(b"BUY 0.6[4]: (5, 4)"),
            s(b"BUY 0.8[1]: (1, 8)"),
            s(b"BUY 0.9[2]: (2, 7) (3, 6)"),
        ]);
        let tree = &borrow_global<MarketBuyTree<FMA, FMB>>(marketAddr).tree;
        assert_market_tree(tree, SIDE_BUY, vector[
            s(b"(0.5 priceLevelID: 5, qty: 3, crankQty: 0)"),
        ]);
        let cache = &borrow_global<MarketBuyCache<FMA, FMB>>(marketAddr).cache;
        assert_market_cache(cache, vector[
            s(b"(0.6 priceLevelID: 4, qty: 4, crankQty: 0)"),
            s(b"(0.8 priceLevelID: 1, qty: 8, crankQty: 0)"),
            s(b"(0.9 priceLevelID: 2, qty: 13, crankQty: 0)"),
        ]);

        // New price should be added to the tree because it falls into the range defined by the tree.
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 4000000000, 100000000000);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert_price_levels(book, vector[
            s(b"BUY 0.4[3]: (4, 10)"),
            s(b"BUY 0.5[5]: (6, 3)"),
            s(b"BUY 0.6[4]: (5, 4)"),
            s(b"BUY 0.8[1]: (1, 8)"),
            s(b"BUY 0.9[2]: (2, 7) (3, 6)"),
        ]);
        let tree = &borrow_global<MarketBuyTree<FMA, FMB>>(marketAddr).tree;
        assert_market_tree(tree, SIDE_BUY, vector[
            s(b"(0.4 priceLevelID: 3, qty: 10, crankQty: 0)"),
            s(b"(0.5 priceLevelID: 5, qty: 3, crankQty: 0)"),
        ]);
        let cache = &borrow_global<MarketBuyCache<FMA, FMB>>(marketAddr).cache;
        assert_market_cache(cache, vector[
            s(b"(0.6 priceLevelID: 4, qty: 4, crankQty: 0)"),
            s(b"(0.8 priceLevelID: 1, qty: 8, crankQty: 0)"),
            s(b"(0.9 priceLevelID: 2, qty: 13, crankQty: 0)"),
        ]);
        assert!(book.summary.buyCacheMax == 9000000000, 0);
        assert!(book.summary.buyCacheMin == 6000000000, 0);
        assert!(book.summary.buyCacheQty == 250000000000, 0);
        assert!(book.summary.buyCacheSize == 3, 0);
        assert!(book.summary.buyTreeMax == 5000000000, 0);
    }

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    fun test_market_price_store_order_inserted_into_price_level_after_executions_buy(aptos: &signer, ferum: &signer, user: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that a sell order is inserted into a price level after executing against other orders.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 4);

        // First insert into both cache and tree, then remove so that there are prices in the tree and in the cache.
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 8000000000, 80000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 9000000000, 70000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 9000000000, 60000000000);
        let orderID = add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 6000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 5000000000, 30000000000);
        cancel_order<FMA, FMB>(user, orderID);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert_price_levels(book, vector[
            s(b"BUY 0.5[5]: (6, 3)"),
            s(b"BUY 0.6[4]: (5, 4)"),
            s(b"BUY 0.8[1]: (1, 8)"),
            s(b"BUY 0.9[2]: (2, 7) (3, 6)"),
        ]);
        let tree = &borrow_global<MarketBuyTree<FMA, FMB>>(marketAddr).tree;
        assert_market_tree(tree, SIDE_BUY, vector[
            s(b"(0.5 priceLevelID: 5, qty: 3, crankQty: 0)"),
        ]);
        let cache = &borrow_global<MarketBuyCache<FMA, FMB>>(marketAddr).cache;
        assert_market_cache(cache, vector[
            s(b"(0.6 priceLevelID: 4, qty: 4, crankQty: 0)"),
            s(b"(0.8 priceLevelID: 1, qty: 8, crankQty: 0)"),
            s(b"(0.9 priceLevelID: 2, qty: 13, crankQty: 0)"),
        ]);

        // New order.
        let newOrderID = add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 8500000000, 160000000000);
        assert!(newOrderID == 4, 0);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert_price_levels(book, vector[
            s(b"BUY 0.5[5]: (6, 3)"),
            s(b"BUY 0.6[4]: (5, 4)"),
            s(b"BUY 0.8[1]: (1, 8)"),
            s(b"BUY 0.9[2]: (2, 7) (3, 6)"),
            s(b"SELL 0.85[3]: (4, 3)"),
        ]);
        let buyTree = &borrow_global<MarketBuyTree<FMA, FMB>>(marketAddr).tree;
        assert_market_tree(buyTree, SIDE_BUY, vector[
            s(b"(0.5 priceLevelID: 5, qty: 3, crankQty: 0)"),
        ]);
        let buyCache = &borrow_global<MarketBuyCache<FMA, FMB>>(marketAddr).cache;
        assert_market_cache(buyCache, vector[
            s(b"(0.6 priceLevelID: 4, qty: 4, crankQty: 0)"),
            s(b"(0.8 priceLevelID: 1, qty: 8, crankQty: 0)"),
            s(b"(0.9 priceLevelID: 2, qty: 0, crankQty: 13)"),
        ]);
        let sellTree = &borrow_global<MarketSellTree<FMA, FMB>>(marketAddr).tree;
        assert_market_tree(sellTree, SIDE_SELL, vector[]);
        let sellCache = &borrow_global<MarketSellCache<FMA, FMB>>(marketAddr).cache;
        assert_market_cache(sellCache, vector[
            s(b"(0.85 priceLevelID: 3, qty: 3, crankQty: 0)"),
        ]);
        assert!(book.summary.buyCacheMax == 9000000000, 0);
        assert!(book.summary.buyCacheMin == 6000000000, 0);
        assert!(book.summary.buyCacheQty == 120000000000, 0);
        assert!(book.summary.buyCacheSize == 3, 0);
        assert!(book.summary.buyTreeMax == 5000000000, 0);
        assert!(book.summary.sellCacheMax == 8500000000, 0);
        assert!(book.summary.sellCacheMin == 8500000000, 0);
        assert!(book.summary.sellCacheQty == 30000000000, 0);
        assert!(book.summary.sellCacheSize == 1, 0);
        assert!(book.summary.sellTreeMin == 0, 0);
    }

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    fun test_market_price_store_price_level_same_price_buy(aptos: &signer, ferum: &signer, user: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that price levels can have the same price on opposite sides.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 4);

        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 8000000000, 80000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 9000000000, 70000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 9000000000, 60000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 6000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 5000000000, 30000000000);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert_price_levels(book, vector[
            s(b"BUY 0.5[5]: (6, 3)"),
            s(b"BUY 0.6[4]: (5, 4)"),
            s(b"BUY 0.7[3]: (4, 5)"),
            s(b"BUY 0.8[1]: (1, 8)"),
            s(b"BUY 0.9[2]: (2, 7) (3, 6)"),
        ]);
        let buyTree = &borrow_global<MarketBuyTree<FMA, FMB>>(marketAddr).tree;
        assert_market_tree(buyTree, SIDE_BUY, vector[
            s(b"(0.5 priceLevelID: 5, qty: 3, crankQty: 0)"),
        ]);
        let buyCache = &borrow_global<MarketBuyCache<FMA, FMB>>(marketAddr).cache;
        assert_market_cache(buyCache, vector[
            s(b"(0.6 priceLevelID: 4, qty: 4, crankQty: 0)"),
            s(b"(0.7 priceLevelID: 3, qty: 5, crankQty: 0)"),
            s(b"(0.8 priceLevelID: 1, qty: 8, crankQty: 0)"),
            s(b"(0.9 priceLevelID: 2, qty: 13, crankQty: 0)"),
        ]);

        // Execute against price levels.
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 5000000000, 250000000000);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert_price_levels(book, vector[
            s(b"BUY 0.5[5]: (6, 3)"),
            s(b"BUY 0.6[4]: (5, 4)"),
            s(b"BUY 0.7[3]: (4, 5)"),
            s(b"BUY 0.8[1]: (1, 8)"),
            s(b"BUY 0.9[2]: (2, 7) (3, 6)"),
        ]);
        let buyTree = &borrow_global<MarketBuyTree<FMA, FMB>>(marketAddr).tree;
        assert_market_tree(buyTree, SIDE_BUY, vector[
            s(b"(0.5 priceLevelID: 5, qty: 3, crankQty: 0)"),
        ]);
        let buyCache = &borrow_global<MarketBuyCache<FMA, FMB>>(marketAddr).cache;
        assert_market_cache(buyCache, vector[
            s(b"(0.6 priceLevelID: 4, qty: 4, crankQty: 0)"),
            s(b"(0.7 priceLevelID: 3, qty: 1, crankQty: 4)"),
            s(b"(0.8 priceLevelID: 1, qty: 0, crankQty: 8)"),
            s(b"(0.9 priceLevelID: 2, qty: 0, crankQty: 13)"),
        ]);
        assert!(book.summary.buyCacheMax == 9000000000, 0);
        assert!(book.summary.buyCacheMin == 6000000000, 0);
        assert!(book.summary.buyCacheQty == 50000000000, 0);
        assert!(book.summary.buyCacheSize == 4, 0);
        assert!(book.summary.buyTreeMax == 5000000000, 0);
        assert!(book.summary.sellCacheMax == 0, 0);
        assert!(book.summary.sellCacheMin == 0, 0);
        assert!(book.summary.sellCacheQty == 0, 0);
        assert!(book.summary.sellCacheSize == 0, 0);
        assert!(book.summary.sellTreeMin == 0, 0);

        // Add sell orders for prices that are already mapped to by price levels.
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 9000000000, 30000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 8000000000, 30000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 7000000000, 30000000000);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert_price_levels(book, vector[
            s(b"BUY 0.5[5]: (6, 3)"),
            s(b"BUY 0.6[4]: (5, 4)"),
            s(b"BUY 0.7[3]: (4, 5)"),
            s(b"BUY 0.8[1]: (1, 8)"),
            s(b"BUY 0.9[2]: (2, 7) (3, 6)"),
            s(b"SELL 0.7[8]: (10, 2)"),
            s(b"SELL 0.8[7]: (9, 3)"),
            s(b"SELL 0.9[6]: (8, 3)"),
        ]);
        let buyTree = &borrow_global<MarketBuyTree<FMA, FMB>>(marketAddr).tree;
        assert_market_tree(buyTree, SIDE_BUY, vector[
            s(b"(0.5 priceLevelID: 5, qty: 3, crankQty: 0)"),
        ]);
        let buyCache = &borrow_global<MarketBuyCache<FMA, FMB>>(marketAddr).cache;
        assert_market_cache(buyCache, vector[
            s(b"(0.6 priceLevelID: 4, qty: 4, crankQty: 0)"),
            s(b"(0.7 priceLevelID: 3, qty: 0, crankQty: 5)"),
            s(b"(0.8 priceLevelID: 1, qty: 0, crankQty: 8)"),
            s(b"(0.9 priceLevelID: 2, qty: 0, crankQty: 13)"),
        ]);
        let sellTree = &borrow_global<MarketSellTree<FMA, FMB>>(marketAddr).tree;
        assert_market_tree(sellTree, SIDE_SELL, vector[]);
        let sellCache = &borrow_global<MarketSellCache<FMA, FMB>>(marketAddr).cache;
        assert_market_cache(sellCache, vector[
            s(b"(0.9 priceLevelID: 6, qty: 3, crankQty: 0)"),
            s(b"(0.8 priceLevelID: 7, qty: 3, crankQty: 0)"),
            s(b"(0.7 priceLevelID: 8, qty: 2, crankQty: 0)"),
        ]);
        assert!(book.summary.buyCacheMax == 9000000000, 0);
        assert!(book.summary.buyCacheMin == 6000000000, 0);
        assert!(book.summary.buyCacheQty == 40000000000, 0);
        assert!(book.summary.buyCacheSize == 4, 0);
        assert!(book.summary.buyTreeMax == 5000000000, 0);
        assert!(book.summary.sellCacheMax == 9000000000, 0);
        assert!(book.summary.sellCacheMin == 7000000000, 0);
        assert!(book.summary.sellCacheQty == 80000000000, 0);
        assert!(book.summary.sellCacheSize == 3, 0);
        assert!(book.summary.sellTreeMin == 0, 0);
    }

    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="Sell side">

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    fun test_market_price_store_cache_overflow_sell(aptos: &signer, ferum: &signer, user: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that added sell prices are put into the tree when the cache is full and the order falls outside the
        // range of the cache. If the cache is full but orders fall into the cache range, it is placed into the cache.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 4);

        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 11234000000, 10000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC,  8000000000, 10240000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC,  9000000000, 11240000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC,  9000000000, 11240000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC,  7000000000, 11240000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC,  7500000000, 11250000000);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert_price_levels(book, vector[
            s(b"SELL 0.7[4]: (5, 1.124)"),
            s(b"SELL 0.75[5]: (6, 1.125)"),
            s(b"SELL 0.8[2]: (2, 1.024)"),
            s(b"SELL 0.9[3]: (3, 1.124) (4, 1.124)"),
            s(b"SELL 1.1234[1]: (1, 1)"),
        ]);
        let tree = &borrow_global<MarketSellTree<FMA, FMB>>(marketAddr).tree;
        assert_market_tree(tree, SIDE_SELL, vector[]);
        let cache = &borrow_global<MarketSellCache<FMA, FMB>>(marketAddr).cache;
        assert_market_cache(cache, vector[
            s(b"(1.1234 priceLevelID: 1, qty: 1, crankQty: 0)"),
            s(b"(0.9 priceLevelID: 3, qty: 2.248, crankQty: 0)"),
            s(b"(0.8 priceLevelID: 2, qty: 1.024, crankQty: 0)"),
            s(b"(0.75 priceLevelID: 5, qty: 1.125, crankQty: 0)"),
            s(b"(0.7 priceLevelID: 4, qty: 1.124, crankQty: 0)"),
        ]);
        assert!(book.summary.sellCacheMax == 11234000000, 0);
        assert!(book.summary.sellCacheMin == 7000000000, 0);
        assert!(book.summary.sellCacheQty == 65210000000, 0);
        assert!(book.summary.sellCacheSize == 5, 0);
        assert!(book.summary.sellTreeMin == 0, 0);

        // These orders should be inserted into the tree.
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC,  26000000000, 11240000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC,  26500000000, 11340000000);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert_price_levels(book, vector[
            s(b"SELL 0.7[4]: (5, 1.124)"),
            s(b"SELL 0.75[5]: (6, 1.125)"),
            s(b"SELL 0.8[2]: (2, 1.024)"),
            s(b"SELL 0.9[3]: (3, 1.124) (4, 1.124)"),
            s(b"SELL 1.1234[1]: (1, 1)"),
            s(b"SELL 2.6[6]: (7, 1.124)"),
            s(b"SELL 2.65[7]: (8, 1.134)"),
        ]);
        let tree = &borrow_global<MarketSellTree<FMA, FMB>>(marketAddr).tree;
        assert_market_tree(tree, SIDE_SELL, vector[
            s(b"(2.65 priceLevelID: 7, qty: 1.134, crankQty: 0)"),
            s(b"(2.6 priceLevelID: 6, qty: 1.124, crankQty: 0)"),
        ]);
        let cache = &borrow_global<MarketSellCache<FMA, FMB>>(marketAddr).cache;
        assert_market_cache(cache, vector[
            s(b"(1.1234 priceLevelID: 1, qty: 1, crankQty: 0)"),
            s(b"(0.9 priceLevelID: 3, qty: 2.248, crankQty: 0)"),
            s(b"(0.8 priceLevelID: 2, qty: 1.024, crankQty: 0)"),
            s(b"(0.75 priceLevelID: 5, qty: 1.125, crankQty: 0)"),
            s(b"(0.7 priceLevelID: 4, qty: 1.124, crankQty: 0)"),
        ]);
        assert!(book.summary.sellCacheMax == 11234000000, 0);
        assert!(book.summary.sellCacheMin == 7000000000, 0);
        assert!(book.summary.sellCacheQty == 65210000000, 0);
        assert!(book.summary.sellCacheSize == 5, 0);
        assert!(book.summary.sellTreeMin == 26000000000, 0);

        // These orders should be inserted into the cache because they fall into the range of the cache.
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC,  1000000000, 11240000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC,  7200000000, 11340000000);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert_price_levels(book, vector[
            s(b"SELL 0.1[8]: (9, 1.124)"),
            s(b"SELL 0.7[4]: (5, 1.124)"),
            s(b"SELL 0.72[9]: (10, 1.134)"),
            s(b"SELL 0.75[5]: (6, 1.125)"),
            s(b"SELL 0.8[2]: (2, 1.024)"),
            s(b"SELL 0.9[3]: (3, 1.124) (4, 1.124)"),
            s(b"SELL 1.1234[1]: (1, 1)"),
            s(b"SELL 2.6[6]: (7, 1.124)"),
            s(b"SELL 2.65[7]: (8, 1.134)")
        ]);
        let tree = &borrow_global<MarketSellTree<FMA, FMB>>(marketAddr).tree;
        assert_market_tree(tree, SIDE_SELL, vector[
            s(b"(2.65 priceLevelID: 7, qty: 1.134, crankQty: 0)"),
            s(b"(2.6 priceLevelID: 6, qty: 1.124, crankQty: 0)"),
        ]);
        let cache = &borrow_global<MarketSellCache<FMA, FMB>>(marketAddr).cache;
        assert_market_cache(cache, vector[
            s(b"(1.1234 priceLevelID: 1, qty: 1, crankQty: 0)"),
            s(b"(0.9 priceLevelID: 3, qty: 2.248, crankQty: 0)"),
            s(b"(0.8 priceLevelID: 2, qty: 1.024, crankQty: 0)"),
            s(b"(0.75 priceLevelID: 5, qty: 1.125, crankQty: 0)"),
            s(b"(0.72 priceLevelID: 9, qty: 1.134, crankQty: 0)"),
            s(b"(0.7 priceLevelID: 4, qty: 1.124, crankQty: 0)"),
            s(b"(0.1 priceLevelID: 8, qty: 1.124, crankQty: 0)"),
        ]);
        assert!(book.summary.sellCacheMax == 11234000000, 0);
        assert!(book.summary.sellCacheMin == 1000000000, 0);
        assert!(book.summary.sellCacheQty == 87790000000, 0);
        assert!(book.summary.sellCacheSize == 7, 0);
        assert!(book.summary.sellTreeMin == 26000000000, 0);
    }

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    fun test_market_price_store_remove_cache_underflow_sell(aptos: &signer, ferum: &signer, user: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that a removed sell price levels doesn't cause prices to move from the cache to the tree.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 4);

        // First insert
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 8000000000, 80000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 9000000000, 70000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 9000000000, 60000000000);
        let orderID = add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 11000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 15000000000, 30000000000);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert_price_levels(book, vector[
            s(b"SELL 0.7[3]: (4, 5)"),
            s(b"SELL 0.8[1]: (1, 8)"),
            s(b"SELL 0.9[2]: (2, 7) (3, 6)"),
            s(b"SELL 1.1[4]: (5, 4)"),
            s(b"SELL 1.5[5]: (6, 3)"),
        ]);
        let tree = &borrow_global<MarketSellTree<FMA, FMB>>(marketAddr).tree;
        assert_market_tree(tree, SIDE_SELL, vector[
            s(b"(1.5 priceLevelID: 5, qty: 3, crankQty: 0)"),
        ]);
        let cache = &borrow_global<MarketSellCache<FMA, FMB>>(marketAddr).cache;
        assert_market_cache(cache, vector[
            s(b"(1.1 priceLevelID: 4, qty: 4, crankQty: 0)"),
            s(b"(0.9 priceLevelID: 2, qty: 13, crankQty: 0)"),
            s(b"(0.8 priceLevelID: 1, qty: 8, crankQty: 0)"),
            s(b"(0.7 priceLevelID: 3, qty: 5, crankQty: 0)"),
        ]);

        // Remove price level from cache by cancelling an order.
        cancel_order<FMA, FMB>(user, orderID);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert_price_levels(book, vector[
            s(b"SELL 0.8[1]: (1, 8)"),
            s(b"SELL 0.9[2]: (2, 7) (3, 6)"),
            s(b"SELL 1.1[4]: (5, 4)"),
            s(b"SELL 1.5[5]: (6, 3)"),
        ]);
        let tree = &borrow_global<MarketSellTree<FMA, FMB>>(marketAddr).tree;
        assert_market_tree(tree, SIDE_SELL, vector[
            s(b"(1.5 priceLevelID: 5, qty: 3, crankQty: 0)"),
        ]);
        let cache = &borrow_global<MarketSellCache<FMA, FMB>>(marketAddr).cache;
        assert_market_cache(cache, vector[
            s(b"(1.1 priceLevelID: 4, qty: 4, crankQty: 0)"),
            s(b"(0.9 priceLevelID: 2, qty: 13, crankQty: 0)"),
            s(b"(0.8 priceLevelID: 1, qty: 8, crankQty: 0)"),
        ]);
        assert!(book.summary.sellCacheMax == 11000000000, 0);
        assert!(book.summary.sellCacheMin == 8000000000, 0);
        assert!(book.summary.sellCacheQty == 250000000000, 0);
        assert!(book.summary.sellCacheSize == 3, 0);
        assert!(book.summary.sellTreeMin == 15000000000, 0);
    }

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    fun test_market_price_store_cache_underflow_add_to_tree_sell(aptos: &signer, ferum: &signer, user: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that a price is added to the tree if it falls into the range defined by the tree, even if the cache
        // has space.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 4);

        // First insert
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 8000000000, 80000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 9000000000, 70000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 9000000000, 60000000000);
        let orderID = add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 11000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 15000000000, 30000000000);
        cancel_order<FMA, FMB>(user, orderID);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert_price_levels(book, vector[
            s(b"SELL 0.8[1]: (1, 8)"),
            s(b"SELL 0.9[2]: (2, 7) (3, 6)"),
            s(b"SELL 1.1[4]: (5, 4)"),
            s(b"SELL 1.5[5]: (6, 3)"),
        ]);
        let tree = &borrow_global<MarketSellTree<FMA, FMB>>(marketAddr).tree;
        assert_market_tree(tree, SIDE_SELL, vector[
            s(b"(1.5 priceLevelID: 5, qty: 3, crankQty: 0)"),
        ]);
        let cache = &borrow_global<MarketSellCache<FMA, FMB>>(marketAddr).cache;
        assert_market_cache(cache, vector[
            s(b"(1.1 priceLevelID: 4, qty: 4, crankQty: 0)"),
            s(b"(0.9 priceLevelID: 2, qty: 13, crankQty: 0)"),
            s(b"(0.8 priceLevelID: 1, qty: 8, crankQty: 0)"),
        ]);

        // New price should be added to the tree because it falls into the range defined by the tree.
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 16000000000, 40000000000);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert_price_levels(book, vector[
            s(b"SELL 0.8[1]: (1, 8)"),
            s(b"SELL 0.9[2]: (2, 7) (3, 6)"),
            s(b"SELL 1.1[4]: (5, 4)"),
            s(b"SELL 1.5[5]: (6, 3)"),
            s(b"SELL 1.6[3]: (4, 4)"),
        ]);
        let tree = &borrow_global<MarketSellTree<FMA, FMB>>(marketAddr).tree;
        assert_market_tree(tree, SIDE_SELL, vector[
            s(b"(1.6 priceLevelID: 3, qty: 4, crankQty: 0)"),
            s(b"(1.5 priceLevelID: 5, qty: 3, crankQty: 0)"),
        ]);
        let cache = &borrow_global<MarketSellCache<FMA, FMB>>(marketAddr).cache;
        assert_market_cache(cache, vector[
            s(b"(1.1 priceLevelID: 4, qty: 4, crankQty: 0)"),
            s(b"(0.9 priceLevelID: 2, qty: 13, crankQty: 0)"),
            s(b"(0.8 priceLevelID: 1, qty: 8, crankQty: 0)"),
        ]);
        assert!(book.summary.sellCacheMax == 11000000000, 0);
        assert!(book.summary.sellCacheMin == 8000000000, 0);
        assert!(book.summary.sellCacheQty == 250000000000, 0);
        assert!(book.summary.sellCacheSize == 3, 0);
        assert!(book.summary.sellTreeMin == 15000000000, 0);
    }

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    fun test_market_price_store_order_inserted_into_price_level_after_executions_sell(aptos: &signer, ferum: &signer, user: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that a buy order is inserted into a price level after executing against other orders.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 4);

        // First insert
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 8000000000, 80000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 9000000000, 70000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 9000000000, 60000000000);
        let orderID = add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 11000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 15000000000, 30000000000);
        cancel_order<FMA, FMB>(user, orderID);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert_price_levels(book, vector[
            s(b"SELL 0.8[1]: (1, 8)"),
            s(b"SELL 0.9[2]: (2, 7) (3, 6)"),
            s(b"SELL 1.1[4]: (5, 4)"),
            s(b"SELL 1.5[5]: (6, 3)"),
        ]);
        let tree = &borrow_global<MarketSellTree<FMA, FMB>>(marketAddr).tree;
        assert_market_tree(tree, SIDE_SELL, vector[
            s(b"(1.5 priceLevelID: 5, qty: 3, crankQty: 0)"),
        ]);
        let cache = &borrow_global<MarketSellCache<FMA, FMB>>(marketAddr).cache;
        assert_market_cache(cache, vector[
            s(b"(1.1 priceLevelID: 4, qty: 4, crankQty: 0)"),
            s(b"(0.9 priceLevelID: 2, qty: 13, crankQty: 0)"),
            s(b"(0.8 priceLevelID: 1, qty: 8, crankQty: 0)"),
        ]);

        // New order.
        let newOrderID = add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 8500000000, 90000000000);
        assert!(newOrderID == 4, 0);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert_price_levels(book, vector[
            s(b"BUY 0.85[3]: (4, 1)"),
            s(b"SELL 0.8[1]: (1, 8)"),
            s(b"SELL 0.9[2]: (2, 7) (3, 6)"),
            s(b"SELL 1.1[4]: (5, 4)"),
            s(b"SELL 1.5[5]: (6, 3)"),
        ]);
        let sellTree = &borrow_global<MarketSellTree<FMA, FMB>>(marketAddr).tree;
        assert_market_tree(sellTree, SIDE_SELL, vector[
            s(b"(1.5 priceLevelID: 5, qty: 3, crankQty: 0)"),
        ]);
        let sellCache = &borrow_global<MarketSellCache<FMA, FMB>>(marketAddr).cache;
        assert_market_cache(sellCache, vector[
            s(b"(1.1 priceLevelID: 4, qty: 4, crankQty: 0)"),
            s(b"(0.9 priceLevelID: 2, qty: 13, crankQty: 0)"),
            s(b"(0.8 priceLevelID: 1, qty: 0, crankQty: 8)"),
        ]);
        let buyTree = &borrow_global<MarketBuyTree<FMA, FMB>>(marketAddr).tree;
        assert_market_tree(buyTree, SIDE_BUY, vector[]);
        let buyCache = &borrow_global<MarketBuyCache<FMA, FMB>>(marketAddr).cache;
        assert_market_cache(buyCache, vector[
            s(b"(0.85 priceLevelID: 3, qty: 1, crankQty: 0)"),
        ]);
        assert!(book.summary.sellCacheMax == 11000000000, 0);
        assert!(book.summary.sellCacheMin == 8000000000, 0);
        assert!(book.summary.sellCacheQty == 170000000000, 0);
        assert!(book.summary.sellCacheSize == 3, 0);
        assert!(book.summary.sellTreeMin == 15000000000, 0);
        assert!(book.summary.buyCacheMax == 8500000000, 0);
        assert!(book.summary.buyCacheMin == 8500000000, 0);
        assert!(book.summary.buyCacheQty == 10000000000, 0);
        assert!(book.summary.buyCacheSize == 1, 0);
        assert!(book.summary.buyTreeMax == 0, 0);
    }

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    fun test_market_price_store_price_level_same_price_sell(aptos: &signer, ferum: &signer, user: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that price levels can have the same price on opposite sides.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 4);

        // First insert
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 8000000000, 80000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 9000000000, 70000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 9000000000, 60000000000);
        let orderID = add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 11000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 15000000000, 30000000000);
        cancel_order<FMA, FMB>(user, orderID);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert_price_levels(book, vector[
            s(b"SELL 0.8[1]: (1, 8)"),
            s(b"SELL 0.9[2]: (2, 7) (3, 6)"),
            s(b"SELL 1.1[4]: (5, 4)"),
            s(b"SELL 1.5[5]: (6, 3)"),
        ]);
        let tree = &borrow_global<MarketSellTree<FMA, FMB>>(marketAddr).tree;
        assert_market_tree(tree, SIDE_SELL, vector[
            s(b"(1.5 priceLevelID: 5, qty: 3, crankQty: 0)"),
        ]);
        let cache = &borrow_global<MarketSellCache<FMA, FMB>>(marketAddr).cache;
        assert_market_cache(cache, vector[
            s(b"(1.1 priceLevelID: 4, qty: 4, crankQty: 0)"),
            s(b"(0.9 priceLevelID: 2, qty: 13, crankQty: 0)"),
            s(b"(0.8 priceLevelID: 1, qty: 8, crankQty: 0)"),
        ]);

        // Execute against sells
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 8500000000, 80000000000);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert_price_levels(book, vector[
            s(b"SELL 0.8[1]: (1, 8)"),
            s(b"SELL 0.9[2]: (2, 7) (3, 6)"),
            s(b"SELL 1.1[4]: (5, 4)"),
            s(b"SELL 1.5[5]: (6, 3)"),
        ]);
        let sellTree = &borrow_global<MarketSellTree<FMA, FMB>>(marketAddr).tree;
        assert_market_tree(sellTree, SIDE_SELL, vector[
            s(b"(1.5 priceLevelID: 5, qty: 3, crankQty: 0)"),
        ]);
        let sellCache = &borrow_global<MarketSellCache<FMA, FMB>>(marketAddr).cache;
        assert_market_cache(sellCache, vector[
            s(b"(1.1 priceLevelID: 4, qty: 4, crankQty: 0)"),
            s(b"(0.9 priceLevelID: 2, qty: 13, crankQty: 0)"),
            s(b"(0.8 priceLevelID: 1, qty: 0, crankQty: 8)"),
        ]);
        assert!(book.summary.sellCacheMax == 11000000000, 0);
        assert!(book.summary.sellCacheMin == 8000000000, 0);
        assert!(book.summary.sellCacheQty == 170000000000, 0);
        assert!(book.summary.sellCacheSize == 3, 0);
        assert!(book.summary.sellTreeMin == 15000000000, 0);
        assert!(book.summary.buyCacheMax == 0, 0);
        assert!(book.summary.buyCacheMin == 0, 0);
        assert!(book.summary.buyCacheQty == 0, 0);
        assert!(book.summary.buyCacheSize == 0, 0);
        assert!(book.summary.buyTreeMax == 0, 0);

        // Add buy orders for prices that are already mapped to by price levels.
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 9000000000, 140000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 8000000000, 60000000000);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert_price_levels(book, vector[
            s(b"BUY 0.8[6]: (8, 6)"),
            s(b"BUY 0.9[3]: (7, 1)"),
            s(b"SELL 0.8[1]: (1, 8)"),
            s(b"SELL 0.9[2]: (2, 7) (3, 6)"),
            s(b"SELL 1.1[4]: (5, 4)"),
            s(b"SELL 1.5[5]: (6, 3)"),
        ]);
        let sellTree = &borrow_global<MarketSellTree<FMA, FMB>>(marketAddr).tree;
        assert_market_tree(sellTree, SIDE_SELL, vector[
            s(b"(1.5 priceLevelID: 5, qty: 3, crankQty: 0)"),
        ]);
        let sellCache = &borrow_global<MarketSellCache<FMA, FMB>>(marketAddr).cache;
        assert_market_cache(sellCache, vector[
            s(b"(1.1 priceLevelID: 4, qty: 4, crankQty: 0)"),
            s(b"(0.9 priceLevelID: 2, qty: 0, crankQty: 13)"),
            s(b"(0.8 priceLevelID: 1, qty: 0, crankQty: 8)"),
        ]);
        let buyTree = &borrow_global<MarketBuyTree<FMA, FMB>>(marketAddr).tree;
        assert_market_tree(buyTree, SIDE_BUY, vector[]);
        let buyCache = &borrow_global<MarketBuyCache<FMA, FMB>>(marketAddr).cache;
        assert_market_cache(buyCache, vector[
            s(b"(0.8 priceLevelID: 6, qty: 6, crankQty: 0)"),
            s(b"(0.9 priceLevelID: 3, qty: 1, crankQty: 0)"),
        ]);
        assert!(book.summary.sellCacheMax == 11000000000, 0);
        assert!(book.summary.sellCacheMin == 8000000000, 0);
        assert!(book.summary.sellCacheQty == 40000000000, 0);
        assert!(book.summary.sellCacheSize == 3, 0);
        assert!(book.summary.sellTreeMin == 15000000000, 0);
        assert!(book.summary.buyCacheMax == 9000000000, 0);
        assert!(book.summary.buyCacheMin == 8000000000, 0);
        assert!(book.summary.buyCacheQty == 70000000000, 0);
        assert!(book.summary.buyCacheSize == 2, 0);
        assert!(book.summary.buyTreeMax == 0, 0);
    }

    // </editor-fold>

    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="Price store execution tests">

    // <editor-fold defaultstate="collapsed" desc="Buy side">

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    fun test_market_price_store_execute_limit_price_exceeded_tree_buy(aptos: &signer, ferum: &signer, user: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that a sell order stops filling when the limit price is exceeded in the tree.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 2);

        // Setup.
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 8000000000, 80000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 9000000000, 70000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 9000000000, 60000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 6000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 5000000000, 30000000000);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.5 qty: 3, crankQty: 0)"),
            s(b"(0.6 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
            s(b"(0.8 qty: 8, crankQty: 0)"),
            s(b"(0.9 qty: 13, crankQty: 0)"),
        ]);
        // New order.
        let newOrderID = add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 6500000000, 300000000000);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.5 qty: 3, crankQty: 0)"),
            s(b"(0.6 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 0, crankQty: 5)"),
        ], vector[
            s(b"(0.8 qty: 0, crankQty: 8)"),
            s(b"(0.9 qty: 0, crankQty: 13)"),
        ]);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[
        ], vector[
            s(b"(0.65 qty: 4, crankQty: 0)"),
        ]);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert!(book.summary.buyCacheMax == 9000000000, 0);
        assert!(book.summary.buyCacheMin == 8000000000, 0);
        assert!(book.summary.buyCacheQty == 0, 0);
        assert!(book.summary.buyCacheSize == 2, 0);
        assert!(book.summary.buyTreeMax == 7000000000, 0);
        assert!(book.summary.sellCacheMax == 6500000000, 0);
        assert!(book.summary.sellCacheMin == 6500000000, 0);
        assert!(book.summary.sellCacheQty == 40000000000, 0);
        assert!(book.summary.sellCacheSize == 1, 0);
        assert!(book.summary.sellTreeMin == 0, 0);
        let newOrder = table::borrow(&book.ordersTable.objects, newOrderID);
        assert!(newOrder.metadata.takerCrankPendingQty == 260000000000, 0);
        assert_exec_events<FMA, FMB>(marketAddr, vector[
            ExecEventInfo {
                qty: 130000000000,
                takerOrderID: newOrderID,
                price: 9000000000,
            },
            ExecEventInfo {
                qty: 80000000000,
                takerOrderID: newOrderID,
                price: 8000000000,
            },
            ExecEventInfo {
                qty: 50000000000,
                takerOrderID: newOrderID,
                price: 7000000000,
            },
        ]);
    }

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    fun test_market_price_store_execute_limit_price_exceeded_cache_buy(aptos: &signer, ferum: &signer, user: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that a sell order stops filling when the limit price is exceeded in the cache.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 2);

        // Setup.
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 8000000000, 80000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 9000000000, 70000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 9000000000, 60000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 6000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 5000000000, 30000000000);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.5 qty: 3, crankQty: 0)"),
            s(b"(0.6 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
            s(b"(0.8 qty: 8, crankQty: 0)"),
            s(b"(0.9 qty: 13, crankQty: 0)"),
        ]);
        // New order.
        let newOrderID = add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 8500000000, 300000000000);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.5 qty: 3, crankQty: 0)"),
            s(b"(0.6 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
            s(b"(0.8 qty: 8, crankQty: 0)"),
            s(b"(0.9 qty: 0, crankQty: 13)"),
        ]);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[
        ], vector[
            s(b"(0.85 qty: 17, crankQty: 0)"),
        ]);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert!(book.summary.buyCacheMax == 9000000000, 0);
        assert!(book.summary.buyCacheMin == 8000000000, 0);
        assert!(book.summary.buyCacheQty == 80000000000, 0);
        assert!(book.summary.buyCacheSize == 2, 0);
        assert!(book.summary.buyTreeMax == 7000000000, 0);
        assert!(book.summary.sellCacheMax == 8500000000, 0);
        assert!(book.summary.sellCacheMin == 8500000000, 0);
        assert!(book.summary.sellCacheQty == 170000000000, 0);
        assert!(book.summary.sellCacheSize == 1, 0);
        assert!(book.summary.sellTreeMin == 0, 0);
        let newOrder = table::borrow(&book.ordersTable.objects, newOrderID);
        assert!(newOrder.metadata.takerCrankPendingQty == 130000000000, 0);
        assert_exec_events<FMA, FMB>(marketAddr, vector[
            ExecEventInfo {
                qty: 130000000000,
                takerOrderID: newOrderID,
                price: 9000000000,
            },
        ]);
    }

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    fun test_market_price_store_execute_qty_exceeded_tree_buy(aptos: &signer, ferum: &signer, user: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that a sell order stops filling when the qty is exceeded in the tree.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 2);

        // Setup.
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 8000000000, 80000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 9000000000, 70000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 9000000000, 60000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 6000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 5000000000, 30000000000);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.5 qty: 3, crankQty: 0)"),
            s(b"(0.6 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
            s(b"(0.8 qty: 8, crankQty: 0)"),
            s(b"(0.9 qty: 13, crankQty: 0)"),
        ]);
        // New order.
        let newOrderID = add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 6500000000, 220000000000);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.5 qty: 3, crankQty: 0)"),
            s(b"(0.6 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 4, crankQty: 1)"),
        ], vector[
            s(b"(0.8 qty: 0, crankQty: 8)"),
            s(b"(0.9 qty: 0, crankQty: 13)"),
        ]);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[], vector[]);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert!(book.summary.buyCacheMax == 9000000000, 0);
        assert!(book.summary.buyCacheMin == 8000000000, 0);
        assert!(book.summary.buyCacheQty == 0, 0);
        assert!(book.summary.buyCacheSize == 2, 0);
        assert!(book.summary.buyTreeMax == 7000000000, 0);
        assert!(book.summary.sellCacheMax == 0, 0);
        assert!(book.summary.sellCacheMin == 0, 0);
        assert!(book.summary.sellCacheQty == 0, 0);
        assert!(book.summary.sellCacheSize == 0, 0);
        assert!(book.summary.sellTreeMin == 0, 0);
        let newOrder = table::borrow(&book.ordersTable.objects, newOrderID);
        assert!(newOrder.metadata.takerCrankPendingQty == 220000000000, 0);
        assert_exec_events<FMA, FMB>(marketAddr, vector[
            ExecEventInfo {
                qty: 130000000000,
                takerOrderID: newOrderID,
                price: 9000000000,
            },
            ExecEventInfo {
                qty: 80000000000,
                takerOrderID: newOrderID,
                price: 8000000000,
            },
            ExecEventInfo {
                qty: 10000000000,
                takerOrderID: newOrderID,
                price: 7000000000,
            },
        ]);
    }

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    fun test_market_price_store_execute_qty_exceeded_cache_buy(aptos: &signer, ferum: &signer, user: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that a sell order stops filling when the qty is exceeded in the cache.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 2);

        // Setup.
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 8000000000, 80000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 9000000000, 70000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 9000000000, 60000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 6000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 5000000000, 30000000000);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.5 qty: 3, crankQty: 0)"),
            s(b"(0.6 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
            s(b"(0.8 qty: 8, crankQty: 0)"),
            s(b"(0.9 qty: 13, crankQty: 0)"),
        ]);
        // New order.
        let newOrderID = add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 6500000000, 10000000000);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.5 qty: 3, crankQty: 0)"),
            s(b"(0.6 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
            s(b"(0.8 qty: 8, crankQty: 0)"),
            s(b"(0.9 qty: 12, crankQty: 1)"),
        ]);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[], vector[]);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert!(book.summary.buyCacheMax == 9000000000, 0);
        assert!(book.summary.buyCacheMin == 8000000000, 0);
        assert!(book.summary.buyCacheQty == 200000000000, 0);
        assert!(book.summary.buyCacheSize == 2, 0);
        assert!(book.summary.buyTreeMax == 7000000000, 0);
        assert!(book.summary.sellCacheMax == 0, 0);
        assert!(book.summary.sellCacheMin == 0, 0);
        assert!(book.summary.sellCacheQty == 0, 0);
        assert!(book.summary.sellCacheSize == 0, 0);
        assert!(book.summary.sellTreeMin == 0, 0);
        let newOrder = table::borrow(&book.ordersTable.objects, newOrderID);
        assert!(newOrder.metadata.takerCrankPendingQty == 10000000000, 0);
        assert_exec_events<FMA, FMB>(marketAddr, vector[
            ExecEventInfo {
                qty: 10000000000,
                takerOrderID: newOrderID,
                price: 9000000000,
            },
        ]);
    }

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    fun test_market_price_store_partial_fill_into_cache_buy(aptos: &signer, ferum: &signer, user: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that a sell order which is partially filled is inserted into the price store.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 2);

        // Setup.
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 8000000000, 80000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 9000000000, 70000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 9000000000, 60000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 6000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 5000000000, 30000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 9500000000, 30000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 10000000000, 30000000000);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.5 qty: 3, crankQty: 0)"),
            s(b"(0.6 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
            s(b"(0.8 qty: 8, crankQty: 0)"),
            s(b"(0.9 qty: 13, crankQty: 0)"),
        ]);
        // New order.
        let newOrderID = add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 8500000000, 300000000000);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.5 qty: 3, crankQty: 0)"),
            s(b"(0.6 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
            s(b"(0.8 qty: 8, crankQty: 0)"),
            s(b"(0.9 qty: 0, crankQty: 13)"),
        ]);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[], vector[
            s(b"(1 qty: 3, crankQty: 0)"),
            s(b"(0.95 qty: 3, crankQty: 0)"),
            s(b"(0.85 qty: 17, crankQty: 0)"),
        ]);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert!(book.summary.buyCacheMax == 9000000000, 0);
        assert!(book.summary.buyCacheMin == 8000000000, 0);
        assert!(book.summary.buyCacheQty == 80000000000, 0);
        assert!(book.summary.buyCacheSize == 2, 0);
        assert!(book.summary.buyTreeMax == 7000000000, 0);
        assert!(book.summary.sellCacheMax ==  10000000000, 0);
        assert!(book.summary.sellCacheMin ==   8500000000, 0);
        assert!(book.summary.sellCacheQty == 230000000000, 0);
        assert!(book.summary.sellCacheSize == 3, 0);
        assert!(book.summary.sellTreeMin == 0, 0);
        let newOrder = table::borrow(&book.ordersTable.objects, newOrderID);
        assert!(newOrder.metadata.takerCrankPendingQty == 130000000000, 0);
        assert_exec_events<FMA, FMB>(marketAddr, vector[
            ExecEventInfo {
                qty: 130000000000,
                takerOrderID: newOrderID,
                price: 9000000000,
            },
        ]);
    }

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    fun test_market_price_store_execute_empty_tree_buy(aptos: &signer, ferum: &signer, user: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that a sell order fills against the cache but doesn't fill against the tree because it is empty.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 4);

        // Setup.
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 8000000000, 80000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 9000000000, 70000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 9000000000, 60000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 6000000000, 40000000000);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
        ], vector[
            s(b"(0.6 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
            s(b"(0.8 qty: 8, crankQty: 0)"),
            s(b"(0.9 qty: 13, crankQty: 0)"),
        ]);
        // New order.
        let newOrderID = add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 5500000000, 310000000000);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
        ], vector[
            s(b"(0.6 qty: 0, crankQty: 4)"),
            s(b"(0.7 qty: 0, crankQty: 5)"),
            s(b"(0.8 qty: 0, crankQty: 8)"),
            s(b"(0.9 qty: 0, crankQty: 13)"),
        ]);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[
        ], vector[
            s(b"(0.55 qty: 1, crankQty: 0)"),
        ]);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert!(book.summary.buyCacheMax == 9000000000, 0);
        assert!(book.summary.buyCacheMin == 6000000000, 0);
        assert!(book.summary.buyCacheQty == 0, 0);
        assert!(book.summary.buyCacheSize == 4, 0);
        assert!(book.summary.buyTreeMax == 0, 0);
        assert!(book.summary.sellCacheMax == 5500000000, 0);
        assert!(book.summary.sellCacheMin == 5500000000, 0);
        assert!(book.summary.sellCacheQty == 10000000000, 0);
        assert!(book.summary.sellCacheSize == 1, 0);
        assert!(book.summary.sellTreeMin == 0, 0);
        let newOrder = table::borrow(&book.ordersTable.objects, newOrderID);
        assert!(newOrder.metadata.takerCrankPendingQty == 300000000000, 0);
        assert_exec_events<FMA, FMB>(marketAddr, vector[
            ExecEventInfo {
                qty: 130000000000,
                takerOrderID: newOrderID,
                price: 9000000000,
            },
            ExecEventInfo {
                qty: 80000000000,
                takerOrderID: newOrderID,
                price: 8000000000,
            },
            ExecEventInfo {
                qty: 50000000000,
                takerOrderID: newOrderID,
                price: 7000000000,
            },
            ExecEventInfo {
                qty: 40000000000,
                takerOrderID: newOrderID,
                price: 6000000000,
            },
        ]);
    }

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    fun test_market_price_store_execute_empty_cache_buy(aptos: &signer, ferum: &signer, user: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that a sell order fills against the tree but not the cache.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 2);

        // Setup.
        let orderIDA = add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 8000000000, 80000000000);
        let orderIDB = add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 9000000000, 70000000000);
        let orderIDC = add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 9000000000, 60000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 6000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 5000000000, 30000000000);
        cancel_order<FMA, FMB>(user, orderIDA);
        cancel_order<FMA, FMB>(user, orderIDB);
        cancel_order<FMA, FMB>(user, orderIDC);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.5 qty: 3, crankQty: 0)"),
            s(b"(0.6 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
        ]);
        // New order.
        let newOrderID = add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 5000000000, 120000000000);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.5 qty: 0, crankQty: 3)"),
            s(b"(0.6 qty: 0, crankQty: 4)"),
            s(b"(0.7 qty: 0, crankQty: 5)"),
        ], vector[
        ]);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert!(book.summary.buyCacheMax == 0, 0);
        assert!(book.summary.buyCacheMin == 0, 0);
        assert!(book.summary.buyCacheQty == 0, 0);
        assert!(book.summary.buyCacheSize == 0, 0);
        assert!(book.summary.buyTreeMax == 7000000000, 0);
        assert!(book.summary.sellCacheMax == 0, 0);
        assert!(book.summary.sellCacheMin == 0, 0);
        assert!(book.summary.sellCacheQty == 0, 0);
        assert!(book.summary.sellCacheSize == 0, 0);
        assert!(book.summary.sellTreeMin == 0, 0);
        let newOrder = table::borrow(&book.ordersTable.objects, newOrderID);
        assert!(newOrder.metadata.takerCrankPendingQty == 120000000000, 0);
        assert_exec_events<FMA, FMB>(marketAddr, vector[
            ExecEventInfo {
                qty: 50000000000,
                takerOrderID: newOrderID,
                price: 7000000000,
            },
            ExecEventInfo {
                qty: 40000000000,
                takerOrderID: newOrderID,
                price: 6000000000,
            },
            ExecEventInfo {
                qty: 30000000000,
                takerOrderID: newOrderID,
                price: 5000000000,
            },
        ]);
    }

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    fun test_market_price_store_execute_against_multiple_cache_buy_levels(aptos: &signer, ferum: &signer, user: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that a sell order executes aganst orders in the cache across multiple price levels. The order ends up
        // being fully filled and eats through levels completely.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 4);

        // Setup.
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 8000000000, 80000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 9000000000, 70000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 9000000000, 60000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 6000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 5000000000, 30000000000);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.5 qty: 3, crankQty: 0)"),
        ], vector[
            s(b"(0.6 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
            s(b"(0.8 qty: 8, crankQty: 0)"),
            s(b"(0.9 qty: 13, crankQty: 0)"),
        ]);
        // New order.
        let newOrderID = add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 5500000000, 210000000000);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.5 qty: 3, crankQty: 0)"),
        ], vector[
            s(b"(0.6 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
            s(b"(0.8 qty: 0, crankQty: 8)"),
            s(b"(0.9 qty: 0, crankQty: 13)"),
        ]);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert!(book.summary.buyCacheMax == 9000000000, 0);
        assert!(book.summary.buyCacheMin == 6000000000, 0);
        assert!(book.summary.buyCacheQty == 90000000000, 0);
        assert!(book.summary.buyCacheSize == 4, 0);
        assert!(book.summary.buyTreeMax == 5000000000, 0);
        assert!(book.summary.sellCacheMax == 0, 0);
        assert!(book.summary.sellCacheMin == 0, 0);
        assert!(book.summary.sellCacheQty == 0, 0);
        assert!(book.summary.sellCacheSize == 0, 0);
        assert!(book.summary.sellTreeMin == 0, 0);
        let newOrder = table::borrow(&book.ordersTable.objects, newOrderID);
        assert!(newOrder.metadata.takerCrankPendingQty == 210000000000, 0);
        assert_exec_events<FMA, FMB>(marketAddr, vector[
            ExecEventInfo {
                qty: 130000000000,
                takerOrderID: newOrderID,
                price: 9000000000,
            },
            ExecEventInfo {
                qty: 80000000000,
                takerOrderID: newOrderID,
                price: 8000000000,
            },
        ]);
    }

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    fun test_market_price_store_execute_against_multiple_cache_single_tree_buy_levels(aptos: &signer, ferum: &signer, user: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that a sell order executes aganst orders in the cache across multiple price levels and orders in the
        // tree in a price level. The order ends up being fully filled and eats through nodes completely.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 2);

        // Setup.
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 8000000000, 80000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 9000000000, 70000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 9000000000, 60000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 6000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 5000000000, 30000000000);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.5 qty: 3, crankQty: 0)"),
            s(b"(0.6 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
            s(b"(0.8 qty: 8, crankQty: 0)"),
            s(b"(0.9 qty: 13, crankQty: 0)"),
        ]);
        // New order.
        let newOrderID = add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 5500000000, 260000000000);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.5 qty: 3, crankQty: 0)"),
            s(b"(0.6 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 0, crankQty: 5)"),
        ], vector[
            s(b"(0.8 qty: 0, crankQty: 8)"),
            s(b"(0.9 qty: 0, crankQty: 13)"),
        ]);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert!(book.summary.buyCacheMax == 9000000000, 0);
        assert!(book.summary.buyCacheMin == 8000000000, 0);
        assert!(book.summary.buyCacheQty == 0, 0);
        assert!(book.summary.buyCacheSize == 2, 0);
        assert!(book.summary.buyTreeMax == 7000000000, 0);
        assert!(book.summary.sellCacheMax == 0, 0);
        assert!(book.summary.sellCacheMin == 0, 0);
        assert!(book.summary.sellCacheQty == 0, 0);
        assert!(book.summary.sellCacheSize == 0, 0);
        assert!(book.summary.sellTreeMin == 0, 0);
        let newOrder = table::borrow(&book.ordersTable.objects, newOrderID);
        assert!(newOrder.metadata.takerCrankPendingQty == 260000000000, 0);
        assert_exec_events<FMA, FMB>(marketAddr, vector[
            ExecEventInfo {
                qty: 130000000000,
                takerOrderID: newOrderID,
                price: 9000000000,
            },
            ExecEventInfo {
                qty: 80000000000,
                takerOrderID: newOrderID,
                price: 8000000000,
            },
            ExecEventInfo {
                qty: 50000000000,
                takerOrderID: newOrderID,
                price: 7000000000,
            },
        ]);
    }

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    fun test_market_price_store_execute_against_multiple_cache_multiple_tree_buy_levels(aptos: &signer, ferum: &signer, user: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that a sell order executes aganst orders in the cache across multiple price levels and orders in the
        // tree in a price level. The order ends up being fully filled and eats through nodes completely.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 2);

        // Setup.
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 8000000000, 80000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 9000000000, 70000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 9000000000, 60000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 6000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 5000000000, 30000000000);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.5 qty: 3, crankQty: 0)"),
            s(b"(0.6 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
            s(b"(0.8 qty: 8, crankQty: 0)"),
            s(b"(0.9 qty: 13, crankQty: 0)"),
        ]);
        // New order.
        let newOrderID = add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 5500000000, 300000000000);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.5 qty: 3, crankQty: 0)"),
            s(b"(0.6 qty: 0, crankQty: 4)"),
            s(b"(0.7 qty: 0, crankQty: 5)"),
        ], vector[
            s(b"(0.8 qty: 0, crankQty: 8)"),
            s(b"(0.9 qty: 0, crankQty: 13)"),
        ]);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert!(book.summary.buyCacheMax == 9000000000, 0);
        assert!(book.summary.buyCacheMin == 8000000000, 0);
        assert!(book.summary.buyCacheQty == 0, 0);
        assert!(book.summary.buyCacheSize == 2, 0);
        assert!(book.summary.buyTreeMax == 7000000000, 0);
        assert!(book.summary.sellCacheMax == 0, 0);
        assert!(book.summary.sellCacheMin == 0, 0);
        assert!(book.summary.sellCacheQty == 0, 0);
        assert!(book.summary.sellCacheSize == 0, 0);
        assert!(book.summary.sellTreeMin == 0, 0);
        let newOrder = table::borrow(&book.ordersTable.objects, newOrderID);
        assert!(newOrder.metadata.takerCrankPendingQty == 300000000000, 0);
        assert_exec_events<FMA, FMB>(marketAddr, vector[
            ExecEventInfo {
                qty: 130000000000,
                takerOrderID: newOrderID,
                price: 9000000000,
            },
            ExecEventInfo {
                qty: 80000000000,
                takerOrderID: newOrderID,
                price: 8000000000,
            },
            ExecEventInfo {
                qty: 50000000000,
                takerOrderID: newOrderID,
                price: 7000000000,
            },
            ExecEventInfo {
                qty: 40000000000,
                takerOrderID: newOrderID,
                price: 6000000000,
            },
        ]);
    }

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    fun test_market_price_store_execute_against_single_cache_buy_levels(aptos: &signer, ferum: &signer, user: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that a sell order executes aganst orders in the cache across a single price level. The order ends up
        // being fully filled and eats through price store level completely.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 4);

        // Setup.
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 8000000000, 80000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 9000000000, 70000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 9000000000, 60000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 6000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 5000000000, 30000000000);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.5 qty: 3, crankQty: 0)")
        ], vector[
            s(b"(0.6 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
            s(b"(0.8 qty: 8, crankQty: 0)"),
            s(b"(0.9 qty: 13, crankQty: 0)"),
        ]);
        // New order.
        let newOrderID = add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 5500000000, 130000000000);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.5 qty: 3, crankQty: 0)")
        ], vector[
            s(b"(0.6 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
            s(b"(0.8 qty: 8, crankQty: 0)"),
            s(b"(0.9 qty: 0, crankQty: 13)"),
        ]);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert!(book.summary.buyCacheMax == 9000000000, 0);
        assert!(book.summary.buyCacheMin == 6000000000, 0);
        assert!(book.summary.buyCacheQty == 170000000000, 0);
        assert!(book.summary.buyCacheSize == 4, 0);
        assert!(book.summary.buyTreeMax == 5000000000, 0);
        assert!(book.summary.sellCacheMax == 0, 0);
        assert!(book.summary.sellCacheMin == 0, 0);
        assert!(book.summary.sellCacheQty == 0, 0);
        assert!(book.summary.sellCacheSize == 0, 0);
        assert!(book.summary.sellTreeMin == 0, 0);
        let newOrder = table::borrow(&book.ordersTable.objects, newOrderID);
        assert!(newOrder.metadata.takerCrankPendingQty == 130000000000, 0);
        assert_exec_events<FMA, FMB>(marketAddr, vector[
            ExecEventInfo {
                qty: 130000000000,
                takerOrderID: newOrderID,
                price: 9000000000,
            },
        ]);
    }

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    fun test_market_price_store_execute_against_single_cache_single_tree_buy_levels(aptos: &signer, ferum: &signer, user: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that a sell order executes aganst orders in the cache across a single price level and orders in the
        // tree in a price level. The order ends up being fully filled and eats through nodes completely.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 2);

        // First insert into both cache and tree, then remove so that there are prices in the tree and only one in
        // the cache.
        let orderIDA = add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 8000000000, 80000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 9000000000, 70000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 9000000000, 60000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 6000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 5000000000, 30000000000);
        cancel_order<FMA, FMB>(user, orderIDA);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.5 qty: 3, crankQty: 0)"),
            s(b"(0.6 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
            s(b"(0.9 qty: 13, crankQty: 0)"),
        ]);

        // New order.
        let newOrderID = add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 5500000000, 180000000000);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.5 qty: 3, crankQty: 0)"),
            s(b"(0.6 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 0, crankQty: 5)"),
        ], vector[
            s(b"(0.9 qty: 0, crankQty: 13)"),
        ]);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert!(book.summary.buyCacheMax == 9000000000, 0);
        assert!(book.summary.buyCacheMin == 9000000000, 0);
        assert!(book.summary.buyCacheQty == 0, 0);
        assert!(book.summary.buyCacheSize == 1, 0);
        assert!(book.summary.buyTreeMax == 7000000000, 0);
        assert!(book.summary.sellCacheMax == 0, 0);
        assert!(book.summary.sellCacheMin == 0, 0);
        assert!(book.summary.sellCacheQty == 0, 0);
        assert!(book.summary.sellCacheSize == 0, 0);
        assert!(book.summary.sellTreeMin == 0, 0);
        let newOrder = table::borrow(&book.ordersTable.objects, newOrderID);
        assert!(newOrder.metadata.takerCrankPendingQty == 180000000000, 0);
        assert_exec_events<FMA, FMB>(marketAddr, vector[
            ExecEventInfo {
                qty: 130000000000,
                takerOrderID: newOrderID,
                price: 9000000000,
            },
            ExecEventInfo {
                qty: 50000000000,
                takerOrderID: newOrderID,
                price: 7000000000,
            },
        ]);
    }

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    fun test_market_price_store_execute_against_single_cache_multiple_tree_buy_levels(aptos: &signer, ferum: &signer, user: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that a sell order executes aganst orders in the cache across a single price level and orders in the
        // tree in a price level. The order ends up being fully filled and eats through nodes completely.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 2);

        // First insert into both cache and tree, then remove so that there are prices in the tree and only one in
        // the cache.
        let orderIDA = add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 8000000000, 80000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 9000000000, 70000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 9000000000, 60000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 6000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 5000000000, 30000000000);
        cancel_order<FMA, FMB>(user, orderIDA);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.5 qty: 3, crankQty: 0)"),
            s(b"(0.6 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
            s(b"(0.9 qty: 13, crankQty: 0)"),
        ]);
        // New order.
        let newOrderID = add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 5500000000, 220000000000);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.5 qty: 3, crankQty: 0)"),
            s(b"(0.6 qty: 0, crankQty: 4)"),
            s(b"(0.7 qty: 0, crankQty: 5)"),
        ], vector[
            s(b"(0.9 qty: 0, crankQty: 13)"),
        ]);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert!(book.summary.buyCacheMax == 9000000000, 0);
        assert!(book.summary.buyCacheMin == 9000000000, 0);
        assert!(book.summary.buyCacheQty == 0, 0);
        assert!(book.summary.buyCacheSize == 1, 0);
        assert!(book.summary.buyTreeMax == 7000000000, 0);
        assert!(book.summary.sellCacheMax == 0, 0);
        assert!(book.summary.sellCacheMin == 0, 0);
        assert!(book.summary.sellCacheQty == 0, 0);
        assert!(book.summary.sellCacheSize == 0, 0);
        assert!(book.summary.sellTreeMin == 0, 0);
        let newOrder = table::borrow(&book.ordersTable.objects, newOrderID);
        assert!(newOrder.metadata.takerCrankPendingQty == 220000000000, 0);
        assert_exec_events<FMA, FMB>(marketAddr, vector[
            ExecEventInfo {
                qty: 130000000000,
                takerOrderID: newOrderID,
                price: 9000000000,
            },
            ExecEventInfo {
                qty: 50000000000,
                takerOrderID: newOrderID,
                price: 7000000000,
            },
            ExecEventInfo {
                qty: 40000000000,
                takerOrderID: newOrderID,
                price: 6000000000,
            },
        ]);
    }

    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="Sell side">

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    fun test_market_price_store_execute_limit_price_exceeded_tree_sell(aptos: &signer, ferum: &signer, user: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that a buy order stops filling when the limit price is exceeded in the tree.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 2);

        // Setup.
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 6000000000, 80000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 5000000000, 70000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 5000000000, 60000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 8000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 9000000000, 30000000000);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.9 qty: 3, crankQty: 0)"),
            s(b"(0.8 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
            s(b"(0.6 qty: 8, crankQty: 0)"),
            s(b"(0.5 qty: 13, crankQty: 0)"),
        ]);
        // New order.
        let newOrderID = add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 6500000000, 300000000000);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.9 qty: 3, crankQty: 0)"),
            s(b"(0.8 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
            s(b"(0.6 qty: 0, crankQty: 8)"),
            s(b"(0.5 qty: 0, crankQty: 13)"),
        ]);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
        ], vector[
            s(b"(0.65 qty: 9, crankQty: 0)"),
        ]);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert!(book.summary.sellCacheMax == 6000000000, 0);
        assert!(book.summary.sellCacheMin == 5000000000, 0);
        assert!(book.summary.sellCacheQty == 0, 0);
        assert!(book.summary.sellCacheSize == 2, 0);
        assert!(book.summary.sellTreeMin == 7000000000, 0);
        assert!(book.summary.buyCacheMax == 6500000000, 0);
        assert!(book.summary.buyCacheMin == 6500000000, 0);
        assert!(book.summary.buyCacheQty == 90000000000, 0);
        assert!(book.summary.buyCacheSize == 1, 0);
        assert!(book.summary.buyTreeMax == 0, 0);
        let newOrder = table::borrow(&book.ordersTable.objects, newOrderID);
        assert!(newOrder.metadata.takerCrankPendingQty == 210000000000, 0);
        assert_exec_events<FMA, FMB>(marketAddr, vector[
            ExecEventInfo {
                qty: 130000000000,
                takerOrderID: newOrderID,
                price: 5000000000,
            },
            ExecEventInfo {
                qty: 80000000000,
                takerOrderID: newOrderID,
                price: 6000000000,
            },
        ]);
    }

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    fun test_market_price_store_execute_limit_price_exceeded_cache_sell(aptos: &signer, ferum: &signer, user: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that a buy order stops filling when the limit price is exceeded in the cache.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 2);

        // Setup.
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 6000000000, 80000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 5000000000, 70000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 5000000000, 60000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 8000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 9000000000, 30000000000);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.9 qty: 3, crankQty: 0)"),
            s(b"(0.8 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
            s(b"(0.6 qty: 8, crankQty: 0)"),
            s(b"(0.5 qty: 13, crankQty: 0)"),
        ]);
        // New order.
        let newOrderID = add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 5500000000, 300000000000);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.9 qty: 3, crankQty: 0)"),
            s(b"(0.8 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
            s(b"(0.6 qty: 8, crankQty: 0)"),
            s(b"(0.5 qty: 0, crankQty: 13)"),
        ]);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
        ], vector[
            s(b"(0.55 qty: 17, crankQty: 0)"),
        ]);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert!(book.summary.sellCacheMax == 6000000000, 0);
        assert!(book.summary.sellCacheMin == 5000000000, 0);
        assert!(book.summary.sellCacheQty == 80000000000, 0);
        assert!(book.summary.sellCacheSize == 2, 0);
        assert!(book.summary.sellTreeMin == 7000000000, 0);
        assert!(book.summary.buyCacheMax == 5500000000, 0);
        assert!(book.summary.buyCacheMin == 5500000000, 0);
        assert!(book.summary.buyCacheQty == 170000000000, 0);
        assert!(book.summary.buyCacheSize == 1, 0);
        assert!(book.summary.buyTreeMax == 0, 0);
        let newOrder = table::borrow(&book.ordersTable.objects, newOrderID);
        assert!(newOrder.metadata.takerCrankPendingQty == 130000000000, 0);
        assert_exec_events<FMA, FMB>(marketAddr, vector[
            ExecEventInfo {
                qty: 130000000000,
                takerOrderID: newOrderID,
                price: 5000000000,
            },
        ]);
    }

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    fun test_market_price_store_execute_qty_exceeded_tree_sell(aptos: &signer, ferum: &signer, user: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that a buy order stops filling when the qty is exceeded in the tree.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 2);

        // Setup.
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 6000000000, 80000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 5000000000, 70000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 5000000000, 60000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 8000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 9000000000, 30000000000);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.9 qty: 3, crankQty: 0)"),
            s(b"(0.8 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
            s(b"(0.6 qty: 8, crankQty: 0)"),
            s(b"(0.5 qty: 13, crankQty: 0)"),
        ]);
        // New order.
        let newOrderID = add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 10000000000, 220000000000);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.9 qty: 3, crankQty: 0)"),
            s(b"(0.8 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 4, crankQty: 1)"),
        ], vector[
            s(b"(0.6 qty: 0, crankQty: 8)"),
            s(b"(0.5 qty: 0, crankQty: 13)"),
        ]);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[], vector[]);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert!(book.summary.sellCacheMax == 6000000000, 0);
        assert!(book.summary.sellCacheMin == 5000000000, 0);
        assert!(book.summary.sellCacheQty == 0, 0);
        assert!(book.summary.sellCacheSize == 2, 0);
        assert!(book.summary.sellTreeMin == 7000000000, 0);
        assert!(book.summary.buyCacheMax == 0, 0);
        assert!(book.summary.buyCacheMin == 0, 0);
        assert!(book.summary.buyCacheQty == 0, 0);
        assert!(book.summary.buyCacheSize == 0, 0);
        assert!(book.summary.buyTreeMax == 0, 0);
        let newOrder = table::borrow(&book.ordersTable.objects, newOrderID);
        assert!(newOrder.metadata.takerCrankPendingQty == 220000000000, 0);
        assert_exec_events<FMA, FMB>(marketAddr, vector[
            ExecEventInfo {
                qty: 130000000000,
                takerOrderID: newOrderID,
                price: 5000000000,
            },
            ExecEventInfo {
                qty: 80000000000,
                takerOrderID: newOrderID,
                price: 6000000000,
            },
            ExecEventInfo {
                qty: 10000000000,
                takerOrderID: newOrderID,
                price: 7000000000,
            },
        ]);
    }

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    fun test_market_price_store_execute_qty_exceeded_cache_sell(aptos: &signer, ferum: &signer, user: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that a buy order stops filling when the qty is exceeded in the cache.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 2);

        // Setup.
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 6000000000, 80000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 5000000000, 70000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 5000000000, 60000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 8000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 9000000000, 30000000000);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.9 qty: 3, crankQty: 0)"),
            s(b"(0.8 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
            s(b"(0.6 qty: 8, crankQty: 0)"),
            s(b"(0.5 qty: 13, crankQty: 0)"),
        ]);
        // New order.
        let newOrderID = add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 10000000000, 140000000000);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.9 qty: 3, crankQty: 0)"),
            s(b"(0.8 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
            s(b"(0.6 qty: 7, crankQty: 1)"),
            s(b"(0.5 qty: 0, crankQty: 13)"),
        ]);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[], vector[]);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert!(book.summary.sellCacheMax == 6000000000, 0);
        assert!(book.summary.sellCacheMin == 5000000000, 0);
        assert!(book.summary.sellCacheQty == 70000000000, 0);
        assert!(book.summary.sellCacheSize == 2, 0);
        assert!(book.summary.sellTreeMin == 7000000000, 0);
        assert!(book.summary.buyCacheMax == 0, 0);
        assert!(book.summary.buyCacheMin == 0, 0);
        assert!(book.summary.buyCacheQty == 0, 0);
        assert!(book.summary.buyCacheSize == 0, 0);
        assert!(book.summary.buyTreeMax == 0, 0);
        let newOrder = table::borrow(&book.ordersTable.objects, newOrderID);
        assert!(newOrder.metadata.takerCrankPendingQty == 140000000000, 0);
        assert_exec_events<FMA, FMB>(marketAddr, vector[
            ExecEventInfo {
                qty: 130000000000,
                takerOrderID: newOrderID,
                price: 5000000000,
            },
            ExecEventInfo {
                qty: 10000000000,
                takerOrderID: newOrderID,
                price: 6000000000,
            },
        ]);
    }

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    fun test_market_price_store_partial_fill_into_cache_sell(aptos: &signer, ferum: &signer, user: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that a buy order stops which is partially filled is inserted into the price store.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 2);

        // Setup.
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 6000000000, 80000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 5000000000, 70000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 5000000000, 60000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 8000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 9000000000, 30000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 4500000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 4000000000, 30000000000);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.9 qty: 3, crankQty: 0)"),
            s(b"(0.8 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
            s(b"(0.6 qty: 8, crankQty: 0)"),
            s(b"(0.5 qty: 13, crankQty: 0)"),
        ]);
        // New order.
        let newOrderID = add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 5500000000, 210000000000);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.9 qty: 3, crankQty: 0)"),
            s(b"(0.8 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
            s(b"(0.6 qty: 8, crankQty: 0)"),
            s(b"(0.5 qty: 0, crankQty: 13)"),
        ]);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[], vector[
            s(b"(0.4 qty: 3, crankQty: 0)"),
            s(b"(0.45 qty: 4, crankQty: 0)"),
            s(b"(0.55 qty: 8, crankQty: 0)"),
        ]);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert!(book.summary.sellCacheMax == 6000000000, 0);
        assert!(book.summary.sellCacheMin == 5000000000, 0);
        assert!(book.summary.sellCacheQty == 80000000000, 0);
        assert!(book.summary.sellCacheSize == 2, 0);
        assert!(book.summary.sellTreeMin == 7000000000, 0);
        assert!(book.summary.buyCacheMax == 5500000000, 0);
        assert!(book.summary.buyCacheMin == 4000000000, 0);
        assert!(book.summary.buyCacheQty == 150000000000, 0);
        assert!(book.summary.buyCacheSize == 3, 0);
        assert!(book.summary.buyTreeMax == 0, 0);
        let newOrder = table::borrow(&book.ordersTable.objects, newOrderID);
        assert!(newOrder.metadata.takerCrankPendingQty == 130000000000, 0);
        assert_exec_events<FMA, FMB>(marketAddr, vector[
            ExecEventInfo {
                qty: 130000000000,
                takerOrderID: newOrderID,
                price: 5000000000,
            },
        ]);
    }

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    fun test_market_price_store_execute_empty_tree_sell(aptos: &signer, ferum: &signer, user: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that a buy order fills against the cache but doesn't fill against the tree because it is empty.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 4);

        // Setup.
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 6000000000, 80000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 7000000000, 70000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 7000000000, 60000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 8000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 9000000000, 40000000000);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[
        ], vector[
            s(b"(0.9 qty: 4, crankQty: 0)"),
            s(b"(0.8 qty: 5, crankQty: 0)"),
            s(b"(0.7 qty: 13, crankQty: 0)"),
            s(b"(0.6 qty: 8, crankQty: 0)"),
        ]);
        // New order.
        let newOrderID = add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 10000000000, 310000000000);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[
        ], vector[
            s(b"(0.9 qty: 0, crankQty: 4)"),
            s(b"(0.8 qty: 0, crankQty: 5)"),
            s(b"(0.7 qty: 0, crankQty: 13)"),
            s(b"(0.6 qty: 0, crankQty: 8)"),
        ]);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
        ], vector[
            s(b"(1 qty: 1, crankQty: 0)"),
        ]);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert!(book.summary.sellCacheMax == 9000000000, 0);
        assert!(book.summary.sellCacheMin == 6000000000, 0);
        assert!(book.summary.sellCacheQty == 0, 0);
        assert!(book.summary.sellCacheSize == 4, 0);
        assert!(book.summary.sellTreeMin == 0, 0);
        assert!(book.summary.buyCacheMax == 10000000000, 0);
        assert!(book.summary.buyCacheMin == 10000000000, 0);
        assert!(book.summary.buyCacheQty == 10000000000, 0);
        assert!(book.summary.buyCacheSize == 1, 0);
        assert!(book.summary.buyTreeMax == 0, 0);
        let newOrder = table::borrow(&book.ordersTable.objects, newOrderID);
        assert!(newOrder.metadata.takerCrankPendingQty == 300000000000, 0);
        assert_exec_events<FMA, FMB>(marketAddr, vector[
            ExecEventInfo {
                qty: 80000000000,
                takerOrderID: newOrderID,
                price: 6000000000,
            },
            ExecEventInfo {
                qty: 130000000000,
                takerOrderID: newOrderID,
                price: 7000000000,
            },
            ExecEventInfo {
                qty: 50000000000,
                takerOrderID: newOrderID,
                price: 8000000000,
            },
            ExecEventInfo {
                qty: 40000000000,
                takerOrderID: newOrderID,
                price: 9000000000,
            },
        ]);
    }

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    fun test_market_price_store_execute_empty_cache_sell(aptos: &signer, ferum: &signer, user: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that a buy order fills against the tree but not the cache.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 2);

        // Setup.
        let orderIDA = add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 5000000000, 80000000000);
        let orderIDB = add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 6000000000, 70000000000);
        let orderIDC = add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 6000000000, 60000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 8000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 9000000000, 30000000000);
        cancel_order<FMA, FMB>(user, orderIDA);
        cancel_order<FMA, FMB>(user, orderIDB);
        cancel_order<FMA, FMB>(user, orderIDC);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.9 qty: 3, crankQty: 0)"),
            s(b"(0.8 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
        ]);
        // New order.
        let newOrderID = add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 10000000000, 120000000000);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.9 qty: 0, crankQty: 3)"),
            s(b"(0.8 qty: 0, crankQty: 4)"),
            s(b"(0.7 qty: 0, crankQty: 5)"),
        ], vector[
        ]);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert!(book.summary.sellCacheMax == 0, 0);
        assert!(book.summary.sellCacheMin == 0, 0);
        assert!(book.summary.sellCacheQty == 0, 0);
        assert!(book.summary.sellCacheSize == 0, 0);
        assert!(book.summary.sellTreeMin == 7000000000, 0);
        assert!(book.summary.buyCacheMax == 0, 0);
        assert!(book.summary.buyCacheMin == 0, 0);
        assert!(book.summary.buyCacheQty == 0, 0);
        assert!(book.summary.buyCacheSize == 0, 0);
        assert!(book.summary.buyTreeMax == 0, 0);
        let newOrder = table::borrow(&book.ordersTable.objects, newOrderID);
        assert!(newOrder.metadata.takerCrankPendingQty == 120000000000, 0);
        assert_exec_events<FMA, FMB>(marketAddr, vector[
            ExecEventInfo {
                qty: 50000000000,
                takerOrderID: newOrderID,
                price: 7000000000,
            },
            ExecEventInfo {
                qty: 40000000000,
                takerOrderID: newOrderID,
                price: 8000000000,
            },
            ExecEventInfo {
                qty: 30000000000,
                takerOrderID: newOrderID,
                price: 9000000000,
            },
        ]);
    }

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    fun test_market_price_store_execute_against_multiple_cache_sell_levels(aptos: &signer, ferum: &signer, user: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that a buy order executes aganst orders in the cache across multiple price levels. The order ends up
        // being fully filled and eats through levels completely.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 4);

        // Setup.
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 6000000000, 80000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 5000000000, 70000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 5000000000, 60000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 8000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 9000000000, 30000000000);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.9 qty: 3, crankQty: 0)"),
        ], vector[
            s(b"(0.8 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
            s(b"(0.6 qty: 8, crankQty: 0)"),
            s(b"(0.5 qty: 13, crankQty: 0)"),
        ]);
        // New order.
        let newOrderID = add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 10000000000, 210000000000);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.9 qty: 3, crankQty: 0)"),
        ], vector[
            s(b"(0.8 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
            s(b"(0.6 qty: 0, crankQty: 8)"),
            s(b"(0.5 qty: 0, crankQty: 13)"),
        ]);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert!(book.summary.sellCacheMax == 8000000000, 0);
        assert!(book.summary.sellCacheMin == 5000000000, 0);
        assert!(book.summary.sellCacheQty == 90000000000, 0);
        assert!(book.summary.sellCacheSize == 4, 0);
        assert!(book.summary.sellTreeMin == 9000000000, 0);
        assert!(book.summary.buyCacheMax == 0, 0);
        assert!(book.summary.buyCacheMin == 0, 0);
        assert!(book.summary.buyCacheQty == 0, 0);
        assert!(book.summary.buyCacheSize == 0, 0);
        assert!(book.summary.buyTreeMax == 0, 0);
        let newOrder = table::borrow(&book.ordersTable.objects, newOrderID);
        assert!(newOrder.metadata.takerCrankPendingQty == 210000000000, 0);
        assert_exec_events<FMA, FMB>(marketAddr, vector[
            ExecEventInfo {
                qty: 130000000000,
                takerOrderID: newOrderID,
                price: 5000000000,
            },
            ExecEventInfo {
                qty: 80000000000,
                takerOrderID: newOrderID,
                price: 6000000000,
            },
        ]);
    }

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    fun test_market_price_store_execute_against_multiple_cache_single_tree_sell_levels(aptos: &signer, ferum: &signer, user: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that a buy order executes aganst orders in the cache across multiple price levels and orders in the
        // tree in a price level. The order ends up being fully filled and eats through nodes completely.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 2);

        // Setup.
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 6000000000, 80000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 5000000000, 70000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 5000000000, 60000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 8000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 9000000000, 30000000000);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.9 qty: 3, crankQty: 0)"),
            s(b"(0.8 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
            s(b"(0.6 qty: 8, crankQty: 0)"),
            s(b"(0.5 qty: 13, crankQty: 0)"),
        ]);
        // New order.
        let newOrderID = add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 10000000000, 260000000000);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.9 qty: 3, crankQty: 0)"),
            s(b"(0.8 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 0, crankQty: 5)"),
        ], vector[
            s(b"(0.6 qty: 0, crankQty: 8)"),
            s(b"(0.5 qty: 0, crankQty: 13)"),
        ]);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert!(book.summary.sellCacheMax == 6000000000, 0);
        assert!(book.summary.sellCacheMin == 5000000000, 0);
        assert!(book.summary.sellCacheQty == 0, 0);
        assert!(book.summary.sellCacheSize == 2, 0);
        assert!(book.summary.sellTreeMin == 7000000000, 0);
        assert!(book.summary.buyCacheMax == 0, 0);
        assert!(book.summary.buyCacheMin == 0, 0);
        assert!(book.summary.buyCacheQty == 0, 0);
        assert!(book.summary.buyCacheSize == 0, 0);
        assert!(book.summary.buyTreeMax == 0, 0);
        let newOrder = table::borrow(&book.ordersTable.objects, newOrderID);
        assert!(newOrder.metadata.takerCrankPendingQty == 260000000000, 0);
        assert_exec_events<FMA, FMB>(marketAddr, vector[
            ExecEventInfo {
                qty: 130000000000,
                takerOrderID: newOrderID,
                price: 5000000000,
            },
            ExecEventInfo {
                qty: 80000000000,
                takerOrderID: newOrderID,
                price: 6000000000,
            },
            ExecEventInfo {
                qty: 50000000000,
                takerOrderID: newOrderID,
                price: 7000000000,
            },
        ]);
    }

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    fun test_market_price_store_execute_against_multiple_cache_multiple_tree_sell_levels(aptos: &signer, ferum: &signer, user: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that a buy order executes aganst orders in the cache across multiple price levels and orders in the
        // tree across multiple price levels. The order ends up being fully filled and eats through nodes completely.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 2);

        // Setup.
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 6000000000, 80000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 5000000000, 70000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 5000000000, 60000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 8000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 9000000000, 30000000000);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.9 qty: 3, crankQty: 0)"),
            s(b"(0.8 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
            s(b"(0.6 qty: 8, crankQty: 0)"),
            s(b"(0.5 qty: 13, crankQty: 0)"),
        ]);
        // New order.
        let newOrderID = add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 10000000000, 300000000000);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.9 qty: 3, crankQty: 0)"),
            s(b"(0.8 qty: 0, crankQty: 4)"),
            s(b"(0.7 qty: 0, crankQty: 5)"),
        ], vector[
            s(b"(0.6 qty: 0, crankQty: 8)"),
            s(b"(0.5 qty: 0, crankQty: 13)"),
        ]);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert!(book.summary.sellCacheMax == 6000000000, 0);
        assert!(book.summary.sellCacheMin == 5000000000, 0);
        assert!(book.summary.sellCacheQty == 0, 0);
        assert!(book.summary.sellCacheSize == 2, 0);
        assert!(book.summary.sellTreeMin == 7000000000, 0);
        assert!(book.summary.buyCacheMax == 0, 0);
        assert!(book.summary.buyCacheMin == 0, 0);
        assert!(book.summary.buyCacheQty == 0, 0);
        assert!(book.summary.buyCacheSize == 0, 0);
        assert!(book.summary.buyTreeMax == 0, 0);
        let newOrder = table::borrow(&book.ordersTable.objects, newOrderID);
        assert!(newOrder.metadata.takerCrankPendingQty == 300000000000, 0);
        assert_exec_events<FMA, FMB>(marketAddr, vector[
            ExecEventInfo {
                qty: 130000000000,
                takerOrderID: newOrderID,
                price: 5000000000,
            },
            ExecEventInfo {
                qty: 80000000000,
                takerOrderID: newOrderID,
                price: 6000000000,
            },
            ExecEventInfo {
                qty: 50000000000,
                takerOrderID: newOrderID,
                price: 7000000000,
            },
            ExecEventInfo {
                qty: 40000000000,
                takerOrderID: newOrderID,
                price: 8000000000,
            },
        ]);
    }

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    fun test_market_price_store_execute_against_single_cache_sell_levels(aptos: &signer, ferum: &signer, user: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that a buy order executes aganst orders in the cache in a single cache level.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 2);

        // Setup.
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 6000000000, 80000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 5000000000, 70000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 5000000000, 60000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 8000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 9000000000, 30000000000);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.9 qty: 3, crankQty: 0)"),
            s(b"(0.8 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
            s(b"(0.6 qty: 8, crankQty: 0)"),
            s(b"(0.5 qty: 13, crankQty: 0)"),
        ]);
        // New order.
        let newOrderID = add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 10000000000, 130000000000);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.9 qty: 3, crankQty: 0)"),
            s(b"(0.8 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
            s(b"(0.6 qty: 8, crankQty: 0)"),
            s(b"(0.5 qty: 0, crankQty: 13)"),
        ]);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert!(book.summary.sellCacheMax == 6000000000, 0);
        assert!(book.summary.sellCacheMin == 5000000000, 0);
        assert!(book.summary.sellCacheQty == 80000000000, 0);
        assert!(book.summary.sellCacheSize == 2, 0);
        assert!(book.summary.sellTreeMin == 7000000000, 0);
        assert!(book.summary.buyCacheMax == 0, 0);
        assert!(book.summary.buyCacheMin == 0, 0);
        assert!(book.summary.buyCacheQty == 0, 0);
        assert!(book.summary.buyCacheSize == 0, 0);
        assert!(book.summary.buyTreeMax == 0, 0);
        let newOrder = table::borrow(&book.ordersTable.objects, newOrderID);
        assert!(newOrder.metadata.takerCrankPendingQty == 130000000000, 0);
        assert_exec_events<FMA, FMB>(marketAddr, vector[
            ExecEventInfo {
                qty: 130000000000,
                takerOrderID: newOrderID,
                price: 5000000000,
            },
        ]);
    }

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    fun test_market_price_store_execute_against_single_cache_single_tree_sell_levels(aptos: &signer, ferum: &signer, user: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that a buy order executes aganst orders in a single cache price level and a single tree price level.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 2);

        // Setup.
        let orderID = add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 6000000000, 80000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 5000000000, 70000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 5000000000, 60000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 8000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 9000000000, 30000000000);
        cancel_order<FMA, FMB>(user, orderID);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.9 qty: 3, crankQty: 0)"),
            s(b"(0.8 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
            s(b"(0.5 qty: 13, crankQty: 0)"),
        ]);
        // New order.
        let newOrderID = add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 10000000000, 180000000000);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.9 qty: 3, crankQty: 0)"),
            s(b"(0.8 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 0, crankQty: 5)"),
        ], vector[
            s(b"(0.5 qty: 0, crankQty: 13)"),
        ]);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert!(book.summary.sellCacheMax == 5000000000, 0);
        assert!(book.summary.sellCacheMin == 5000000000, 0);
        assert!(book.summary.sellCacheQty == 0, 0);
        assert!(book.summary.sellCacheSize == 1, 0);
        assert!(book.summary.sellTreeMin == 7000000000, 0);
        assert!(book.summary.buyCacheMax == 0, 0);
        assert!(book.summary.buyCacheMin == 0, 0);
        assert!(book.summary.buyCacheQty == 0, 0);
        assert!(book.summary.buyCacheSize == 0, 0);
        assert!(book.summary.buyTreeMax == 0, 0);
        let newOrder = table::borrow(&book.ordersTable.objects, newOrderID);
        assert!(newOrder.metadata.takerCrankPendingQty == 180000000000, 0);
        assert_exec_events<FMA, FMB>(marketAddr, vector[
            ExecEventInfo {
                qty: 130000000000,
                takerOrderID: newOrderID,
                price: 5000000000,
            },
            ExecEventInfo {
                qty: 50000000000,
                takerOrderID: newOrderID,
                price: 7000000000,
            },
            ExecEventInfo {
                qty: 10000000000,
                takerOrderID: newOrderID,
                price: 7000000000,
            },
        ]);
    }

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    fun test_market_price_store_execute_against_single_cache_multiple_tree_sell_levels(aptos: &signer, ferum: &signer, user: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that a buy order executes aganst orders in a single cache price level and multiple tree price level.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 2);

        // Setup.
        let orderID = add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 6000000000, 80000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 5000000000, 70000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 5000000000, 60000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 8000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 9000000000, 30000000000);
        cancel_order<FMA, FMB>(user, orderID);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.9 qty: 3, crankQty: 0)"),
            s(b"(0.8 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
            s(b"(0.5 qty: 13, crankQty: 0)"),
        ]);
        // New order.
        let newOrderID = add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 10000000000, 220000000000);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.9 qty: 3, crankQty: 0)"),
            s(b"(0.8 qty: 0, crankQty: 4)"),
            s(b"(0.7 qty: 0, crankQty: 5)"),
        ], vector[
            s(b"(0.5 qty: 0, crankQty: 13)"),
        ]);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert!(book.summary.sellCacheMax == 5000000000, 0);
        assert!(book.summary.sellCacheMin == 5000000000, 0);
        assert!(book.summary.sellCacheQty == 0, 0);
        assert!(book.summary.sellCacheSize == 1, 0);
        assert!(book.summary.sellTreeMin == 7000000000, 0);
        assert!(book.summary.buyCacheMax == 0, 0);
        assert!(book.summary.buyCacheMin == 0, 0);
        assert!(book.summary.buyCacheQty == 0, 0);
        assert!(book.summary.buyCacheSize == 0, 0);
        assert!(book.summary.buyTreeMax == 0, 0);
        let newOrder = table::borrow(&book.ordersTable.objects, newOrderID);
        assert!(newOrder.metadata.takerCrankPendingQty == 220000000000, 0);
        assert_exec_events<FMA, FMB>(marketAddr, vector[
            ExecEventInfo {
                qty: 130000000000,
                takerOrderID: newOrderID,
                price: 5000000000,
            },
            ExecEventInfo {
                qty: 50000000000,
                takerOrderID: newOrderID,
                price: 7000000000,
            },
            ExecEventInfo {
                qty: 40000000000,
                takerOrderID: newOrderID,
                price: 8000000000,
            },
        ]);
    }

    // </editor-fold>

    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="Cancel order tests">

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    fun test_market_cancel_full_tree(aptos: &signer, ferum: &signer, user: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that an order can be cancelled from the tree.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 2);

        // Setup.
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 8000000000, 80000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 9000000000, 70000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 9000000000, 60000000000);
        let orderID = add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 6000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 5000000000, 30000000000);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.5 qty: 3, crankQty: 0)"),
            s(b"(0.6 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
            s(b"(0.8 qty: 8, crankQty: 0)"),
            s(b"(0.9 qty: 13, crankQty: 0)"),
        ]);

        // Cancel order.
        cancel_order<FMA, FMB>(user, orderID);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.5 qty: 3, crankQty: 0)"),
            s(b"(0.6 qty: 4, crankQty: 0)"),
        ], vector[
            s(b"(0.8 qty: 8, crankQty: 0)"),
            s(b"(0.9 qty: 13, crankQty: 0)"),
        ]);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert!(book.summary.buyCacheMax == 9000000000, 0);
        assert!(book.summary.buyCacheMin == 8000000000, 0);
        assert!(book.summary.buyCacheQty == 210000000000, 0);
        assert!(book.summary.buyCacheSize == 2, 0);
        assert!(book.summary.buyTreeMax == 6000000000, 0);
        assert_order_collateral(book, orderID, 0, 0);
        assert_order_unused(book, orderID);
        assert_order_qtys<FMA, FMB>(marketAddr, orderID, 0, 0, 0);
    }

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    fun test_market_cancel_full_cache(aptos: &signer, ferum: &signer, user: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that an order can be cancelled from the cache.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 2);

        // Setup.
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 8000000000, 80000000000);
        let orderID = add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 9000000000, 70000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 9000000000, 60000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 6000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 5000000000, 30000000000);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.5 qty: 3, crankQty: 0)"),
            s(b"(0.6 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
            s(b"(0.8 qty: 8, crankQty: 0)"),
            s(b"(0.9 qty: 13, crankQty: 0)"),
        ]);

        // Cancel order.
        cancel_order<FMA, FMB>(user, orderID);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.5 qty: 3, crankQty: 0)"),
            s(b"(0.6 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
            s(b"(0.8 qty: 8, crankQty: 0)"),
            s(b"(0.9 qty: 6, crankQty: 0)"),
        ]);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert!(book.summary.buyCacheMax == 9000000000, 0);
        assert!(book.summary.buyCacheMin == 8000000000, 0);
        assert!(book.summary.buyCacheQty == 140000000000, 0);
        assert!(book.summary.buyCacheSize == 2, 0);
        assert!(book.summary.buyTreeMax == 7000000000, 0);
        assert_order_collateral(book, orderID, 0, 0);
        assert_order_unused(book, orderID);
        assert_order_qtys<FMA, FMB>(marketAddr, orderID, 0, 0, 0);
    }

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    fun test_market_cancel_partial_tree(aptos: &signer, ferum: &signer, user: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that an order can be partially cancelled from the tree.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 2);

        // Setup.
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 8000000000, 80000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 9000000000, 70000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 9000000000, 60000000000);
        let orderID = add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 6000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 5000000000, 30000000000);
        // Execute partially.
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 6500000000, 230000000000);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.5 qty: 3, crankQty: 0)"),
            s(b"(0.6 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 3, crankQty: 2)"),
        ], vector[
            s(b"(0.8 qty: 0, crankQty: 8)"),
            s(b"(0.9 qty: 0, crankQty: 13)"),
        ]);

        // Cancel order.
        cancel_order<FMA, FMB>(user, orderID);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.5 qty: 3, crankQty: 0)"),
            s(b"(0.6 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 0, crankQty: 2)"),
        ], vector[
            s(b"(0.8 qty: 0, crankQty: 8)"),
            s(b"(0.9 qty: 0, crankQty: 13)"),
        ]);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert!(book.summary.buyCacheMax == 9000000000, 0);
        assert!(book.summary.buyCacheMin == 8000000000, 0);
        assert!(book.summary.buyCacheQty == 0, 0);
        assert!(book.summary.buyCacheSize == 2, 0);
        assert!(book.summary.buyTreeMax == 7000000000, 0);
        assert_order_collateral(book, orderID, 14000000000, 0);
        assert_order_used(book, orderID);
        assert_order_qtys<FMA, FMB>(marketAddr, orderID, 20000000000, 0, 20000000000);
    }

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    fun test_market_cancel_partial_cache(aptos: &signer, ferum: &signer, user: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that an order can be partially cancelled from the cache.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 2);

        // Setup.
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 8000000000, 80000000000);
        let orderID = add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 9000000000, 70000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 9000000000, 60000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 6000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 5000000000, 30000000000);
        // Execute partially.
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 8500000000, 30000000000);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.5 qty: 3, crankQty: 0)"),
            s(b"(0.6 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
            s(b"(0.8 qty: 8, crankQty: 0)"),
            s(b"(0.9 qty: 10, crankQty: 3)"),
        ]);

        // Cancel order.
        cancel_order<FMA, FMB>(user, orderID);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.5 qty: 3, crankQty: 0)"),
            s(b"(0.6 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
            s(b"(0.8 qty: 8, crankQty: 0)"),
            s(b"(0.9 qty: 6, crankQty: 3)"),
        ]);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert!(book.summary.buyCacheMax == 9000000000, 0);
        assert!(book.summary.buyCacheMin == 8000000000, 0);
        assert!(book.summary.buyCacheQty == 140000000000, 0);
        assert!(book.summary.buyCacheSize == 2, 0);
        assert!(book.summary.buyTreeMax == 7000000000, 0);
        assert_order_collateral(book, orderID, 27000000000, 0);
        assert_order_used(book, orderID);
        assert_order_qtys<FMA, FMB>(marketAddr, orderID, 30000000000, 0, 30000000000);
    }

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    fun test_market_cancel_partial_pending_taker(aptos: &signer, ferum: &signer, user: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that an order can be partially cancelled if it has some pending taker qty.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 2);

        // Setup.
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 8500000000, 30000000000); // Maker order.
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 8000000000, 80000000000);
        let orderID = add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 9000000000, 70000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 9000000000, 60000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 6000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 5000000000, 30000000000);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.5 qty: 3, crankQty: 0)"),
            s(b"(0.6 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
            s(b"(0.8 qty: 8, crankQty: 0)"),
            s(b"(0.9 qty: 10, crankQty: 0)"),
        ]);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[], vector[
            s(b"(0.85 qty: 0, crankQty: 3)"),
        ]);
        assert_order_qtys<FMA, FMB>(marketAddr, orderID, 70000000000, 30000000000, 0);

        // Cancel order.
        cancel_order<FMA, FMB>(user, orderID);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.5 qty: 3, crankQty: 0)"),
            s(b"(0.6 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
            s(b"(0.8 qty: 8, crankQty: 0)"),
            s(b"(0.9 qty: 6, crankQty: 0)"),
        ]);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[], vector[
            s(b"(0.85 qty: 0, crankQty: 3)"),
        ]);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert!(book.summary.buyCacheMax == 9000000000, 0);
        assert!(book.summary.buyCacheMin == 8000000000, 0);
        assert!(book.summary.buyCacheQty == 140000000000, 0);
        assert!(book.summary.buyCacheSize == 2, 0);
        assert!(book.summary.buyTreeMax == 7000000000, 0);
        assert_order_collateral(book, orderID, 27000000000, 0);
        assert_order_used(book, orderID);
        assert_order_qtys<FMA, FMB>(marketAddr, orderID, 30000000000, 30000000000, 0);
    }

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    fun test_market_cancel_partial_pending_maker(aptos: &signer, ferum: &signer, user: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that an order can be partially cancelled if it has some pending maker qty.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 2);

        // Setup.
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 8000000000, 80000000000);
        let orderID = add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 9000000000, 70000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 9000000000, 60000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 6000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 5000000000, 30000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 8500000000, 30000000000); // Taker order.
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.5 qty: 3, crankQty: 0)"),
            s(b"(0.6 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
            s(b"(0.8 qty: 8, crankQty: 0)"),
            s(b"(0.9 qty: 10, crankQty: 3)"),
        ]);
        assert_order_qtys<FMA, FMB>(marketAddr, orderID, 70000000000, 0, 30000000000);

        // Cancel order.
        cancel_order<FMA, FMB>(user, orderID);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.5 qty: 3, crankQty: 0)"),
            s(b"(0.6 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
            s(b"(0.8 qty: 8, crankQty: 0)"),
            s(b"(0.9 qty: 6, crankQty: 3)"),
        ]);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert!(book.summary.buyCacheMax == 9000000000, 0);
        assert!(book.summary.buyCacheMin == 8000000000, 0);
        assert!(book.summary.buyCacheQty == 140000000000, 0);
        assert!(book.summary.buyCacheSize == 2, 0);
        assert!(book.summary.buyTreeMax == 7000000000, 0);
        assert_order_collateral(book, orderID, 27000000000, 0);
        assert_order_used(book, orderID);
        assert_order_qtys<FMA, FMB>(marketAddr, orderID, 30000000000, 0, 30000000000);
    }

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    fun test_market_cancel_partial_pending_both_maker_and_taker_buy(aptos: &signer, ferum: &signer, user: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that a buy order can be partially cancelled if it has both maker and taker pending qtys.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 2);

        // Setup.
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 8500000000, 10000000000); // Maker order.
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 8000000000, 80000000000);
        let orderID = add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 9000000000, 70000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 6000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 5000000000, 30000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 8500000000, 30000000000); // Taker order.
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.5 qty: 3, crankQty: 0)"),
            s(b"(0.6 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
            s(b"(0.8 qty: 8, crankQty: 0)"),
            s(b"(0.9 qty: 3, crankQty: 3)"),
        ]);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[], vector[
            s(b"(0.85 qty: 0, crankQty: 1)"),
        ]);
        assert_order_qtys<FMA, FMB>(marketAddr, orderID, 70000000000, 10000000000, 30000000000);

        // Cancel order.
        cancel_order<FMA, FMB>(user, orderID);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.5 qty: 3, crankQty: 0)"),
            s(b"(0.6 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
            s(b"(0.8 qty: 8, crankQty: 0)"),
            s(b"(0.9 qty: 0, crankQty: 3)"),
        ]);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[], vector[
            s(b"(0.85 qty: 0, crankQty: 1)"),
        ]);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert!(book.summary.buyCacheMax == 9000000000, 0);
        assert!(book.summary.buyCacheMin == 8000000000, 0);
        assert!(book.summary.buyCacheQty == 80000000000, 0);
        assert!(book.summary.buyCacheSize == 2, 0);
        assert!(book.summary.buyTreeMax == 7000000000, 0);
        assert_order_collateral(book, orderID, 36000000000, 0);
        assert_order_used(book, orderID);
        assert_order_qtys<FMA, FMB>(marketAddr, orderID, 40000000000, 10000000000, 30000000000);
    }

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    fun test_market_cancel_partial_pending_both_maker_and_taker_sell(aptos: &signer, ferum: &signer, user: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that a sell order can be partially cancelled if it has both maker and taker pending qtys.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 2);

        // Setup.
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 5500000000, 10000000000); // Maker order.
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 6000000000, 80000000000);
        let orderID = add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 5000000000, 70000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 8000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 9000000000, 30000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 5500000000, 30000000000); // Taker order.
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.9 qty: 3, crankQty: 0)"),
            s(b"(0.8 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
            s(b"(0.6 qty: 8, crankQty: 0)"),
            s(b"(0.5 qty: 3, crankQty: 3)"),
        ]);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[], vector[
            s(b"(0.55 qty: 0, crankQty: 1)"),
        ]);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert_order_collateral(book, orderID, 0, 70000000000);
        assert_order_qtys<FMA, FMB>(marketAddr, orderID, 70000000000, 10000000000, 30000000000);

        // Cancel order.
        cancel_order<FMA, FMB>(user, orderID);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.9 qty: 3, crankQty: 0)"),
            s(b"(0.8 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
            s(b"(0.6 qty: 8, crankQty: 0)"),
            s(b"(0.5 qty: 0, crankQty: 3)"),
        ]);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[], vector[
            s(b"(0.55 qty: 0, crankQty: 1)"),
        ]);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        assert!(book.summary.sellCacheMax == 6000000000, 0);
        assert!(book.summary.sellCacheMin == 5000000000, 0);
        assert!(book.summary.sellCacheQty == 80000000000, 0);
        assert!(book.summary.sellCacheSize == 2, 0);
        assert!(book.summary.sellTreeMin == 7000000000, 0);
        assert_order_collateral(book, orderID, 0, 40000000000);
        assert_order_used(book, orderID);
        assert_order_qtys<FMA, FMB>(marketAddr, orderID, 40000000000, 10000000000, 30000000000);
    }

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    #[expected_failure(abort_code=ERR_ORDER_EXECUTED_BUT_IS_PENDING_CRANK)]
    fun test_market_cancel_fail_pending_maker(aptos: &signer, ferum: &signer, user: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that an order with maker pending crank qty can't be cancelled.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 2);

        // Setup.
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 8000000000, 80000000000);
        let orderID = add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 9000000000, 70000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 6000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 5000000000, 30000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 8500000000, 70000000000); // Taker order.
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.5 qty: 3, crankQty: 0)"),
            s(b"(0.6 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
            s(b"(0.8 qty: 8, crankQty: 0)"),
            s(b"(0.9 qty: 0, crankQty: 7)"),
        ]);
        assert_order_qtys<FMA, FMB>(marketAddr, orderID, 70000000000, 0, 70000000000);

        // Cancel order.
        cancel_order<FMA, FMB>(user, orderID);
    }

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    #[expected_failure(abort_code=ERR_ORDER_EXECUTED_BUT_IS_PENDING_CRANK)]
    fun test_market_cancel_fail_pending_taker(aptos: &signer, ferum: &signer, user: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that an order with taker pending crank qty can't be cancelled.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 2);

        // Setup.
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 8500000000, 70000000000); // Maker order.
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 8000000000, 80000000000);
        let orderID = add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 9000000000, 70000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 6000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 5000000000, 30000000000);
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.5 qty: 3, crankQty: 0)"),
            s(b"(0.6 qty: 4, crankQty: 0)"),
        ], vector[
            s(b"(0.7 qty: 5, crankQty: 0)"),
            s(b"(0.8 qty: 8, crankQty: 0)"),
        ]);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[], vector[
            s(b"(0.85 qty: 0, crankQty: 7)"),
        ]);
        assert_order_qtys<FMA, FMB>(marketAddr, orderID, 70000000000, 70000000000, 0);

        // Cancel order.
        cancel_order<FMA, FMB>(user, orderID);
    }

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    #[expected_failure(abort_code=ERR_ORDER_EXECUTED_BUT_IS_PENDING_CRANK)]
    fun test_market_cancel_fail_pending_maker_and_taker(aptos: &signer, ferum: &signer, user: &signer)
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        // Tests that an order with maker and taker pending crank qty can't be cancelled.

        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 2);

        // Setup.
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 8500000000, 40000000000); // Maker order.
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 8000000000, 80000000000);
        let orderID = add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 9000000000, 70000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 7000000000, 50000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 6000000000, 40000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_BUY, BEHAVIOUR_GTC, 5000000000, 30000000000);
        add_user_limit_order<FMA, FMB>(user, SIDE_SELL, BEHAVIOUR_GTC, 8500000000, 30000000000); // Taker order.
        assert_buy_price_store_qtys<FMA, FMB>(marketAddr, vector[
            s(b"(0.5 qty: 3, crankQty: 0)"),
            s(b"(0.6 qty: 4, crankQty: 0)"),
            s(b"(0.7 qty: 5, crankQty: 0)"),
        ], vector[
            s(b"(0.8 qty: 8, crankQty: 0)"),
            s(b"(0.9 qty: 0, crankQty: 3)"),
        ]);
        assert_sell_price_store_qtys<FMA, FMB>(marketAddr, vector[], vector[
            s(b"(0.85 qty: 0, crankQty: 4)"),
        ]);
        assert_order_qtys<FMA, FMB>(marketAddr, orderID, 70000000000, 40000000000, 30000000000);

        // Cancel order.
        cancel_order<FMA, FMB>(user, orderID);
    }

    // </editor-fold>

    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="Market Test utils">

    #[test(aptos=@0x1, ferum=@ferum, user=@0x3)]
    fun test_setup_ferum(aptos: &signer, ferum: &signer, user: &signer)
       acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        let marketAddr = setup_ferum_test<FMA, FMB>(aptos, ferum, user, 8);
        assert!(marketAddr == get_market_addr<FMA, FMB>(), 0);
        assert!(marketAddr == address_of(ferum), 0);
        let book = borrow_global<Orderbook<FMA, FMB>>(marketAddr);
        let userAccIdentifier = platform::account_identifier_for_test(user);
        let accountKey = account_key_from_identifier(userAccIdentifier);
        assert!(table::contains(&book.marketAccounts, accountKey), 0);
        assert_account_balances(book, accountKey, 1000000000000, 1000000000000);
    }

    #[test_only]
    fun print_price_levels<I, Q>(book: &Orderbook<I, Q>) {
        debug::print(&price_levels_str(book));
    }

    #[test_only]
    fun assert_price_levels<I, Q>(book: &Orderbook<I, Q>, expected: vector<string::String>) {
        if (price_levels_str(book) != expected) {
            debug::print(&s(b"Actual Price Levels"));
            print_price_levels(book);
            abort 0
        };
    }

    struct PriceLevelInfo has copy, drop, store {
        orders: vector<PriceLevelOrder>,
        priceLevelID: u16,
    }

    struct PriceLevelInfoKey has copy, drop, store {
        price: u64,
        side: u8,
    }

    struct ExecEventInfo has drop, copy {
        qty: u64,
        takerOrderID: u32,
        price: u64,
    }

    #[test_only]
    fun assert_exec_events<I, Q>(marketAddr: address, events: vector<ExecEventInfo>) acquires EventQueue, Orderbook {
        let book = borrow_global<Orderbook<I, Q>>(marketAddr);
        let ordersTable = &book.ordersTable.objects;
        let priceLevels = &book.priceLevelsTable.objects;
        let queue = &borrow_global<EventQueue<I, Q>>(marketAddr).queue;
        let it = list_iterate(queue);
        let i = 0;
        while (it.nodeID != 0) {
            let elem = list_get_next(queue, &mut it);
            let expected = vector::borrow(&events, i);
            assert!(elem.qty == expected.qty, 0);
            assert!(elem.takerOrderID == expected.takerOrderID, 0);
            let priceLevel = table::borrow(priceLevels, elem.priceLevelID);
            let orders = list_to_vector(&priceLevel.orders);
            assert!(vector::length(&orders) > 0, 0);
            let price = table::borrow(ordersTable, vector::borrow(&orders, 0).id).metadata.price;
            assert!(price == expected.price, 0);
            let qtySum = 0;
            while (!vector::is_empty(&orders)) {
                qtySum = qtySum + vector::pop_back(&mut orders).qty;
            };
            assert!(qtySum >= expected.qty, 0);
            i = i + 1;
        };
    }

    #[test_only]
    fun price_levels_str<I, Q>(book: &Orderbook<I, Q>): vector<string::String> {
        let priceLevelsTable = &book.priceLevelsTable;
        // First find out all the unused price level nodes.
        let unusedPriceLevels = table::new();
        let currNodeID = priceLevelsTable.unusedStack;
        while (currNodeID != 0) {
            let node = table::borrow(&priceLevelsTable.objects, currNodeID);
            table::add(&mut unusedPriceLevels, currNodeID, true);
            currNodeID = node.next;
        };
        // Now, use that set to filter out all posible node IDs.
        let usedNodes = vector<u16>[];
        let i: u64 = 1;
        while (i < (priceLevelsTable.currID as u64)) {
            if (!table::contains(&unusedPriceLevels, (i as u16))) {
                vector::push_back(&mut usedNodes, (i as u16));
            };
            i = i + 1;
        };
        table::drop_unchecked(unusedPriceLevels);
        // Get all the elements from the nodes.
        let keys = vector[];
        let elements = table::new();
        let size = vector::length(&usedNodes);
        i = 0;
        while (i < size) {
            let nodeID = vector::pop_back(&mut usedNodes);
            let node = table::borrow(&priceLevelsTable.objects, nodeID);
            let nodeElems = list_to_vector(&node.orders);
            // Determine the price this node corresponds to by looking at an order.
            let orderID = vector::borrow(&nodeElems, 0).id;
            let order = table::borrow(&book.ordersTable.objects, orderID);
            let price = order.metadata.price;
            let side = order.metadata.side;
            let key = PriceLevelInfoKey { price, side };
            if (!table::contains(&elements, key)) {
                table::add(&mut elements, key, PriceLevelInfo {
                    orders: vector[],
                    priceLevelID: nodeID,
                });
                vector::push_back(&mut keys, key);
            };
            vector::append(&mut table::borrow_mut(&mut elements, key).orders, nodeElems);
            i = i + 1;
        };
        // Create string.
        sort_price_level_info_keys(&mut keys);
        let i = 0;
        let size = vector::length(&keys);
        let output = vector[];
        while (i < size) {
            let key = *vector::borrow(&keys, i);
            let info = table::remove(&mut elements, key);
            let elemStr = price_level_orders_to_str(info.orders);
            let str = if (key.side == SIDE_BUY) {
                s(b"BUY ")
            } else {
                s(b"SELL ")
            };
            string::append(&mut str, ftu::pretty_print_fp(key.price, DECIMAL_PLACES));
            string::append_utf8(&mut str, b"[");
            string::append(&mut str, ftu::u16_to_string(info.priceLevelID));
            string::append_utf8(&mut str, b"]: ");
            string::append(&mut str, elemStr);
            vector::push_back(&mut output, str);
            i = i + 1;
        };
        table::drop_unchecked(elements);
        output
    }

    #[test_only]
    public fun sort_price_level_info_keys(vec: &mut vector<PriceLevelInfoKey>) {
        let i = 0;
        let size = vector::length(vec);
        if (size <= 1) {
            return
        };
        while (i < size) {
            let j = 0;
            while (j < size - 1) {
                let a = vector::borrow(vec, j);
                let b = vector::borrow(vec, j + 1);
                if (a.side > b.side) {
                    vector::swap(vec, j, j+1);
                } else if (a.side == b.side && a.price > b.price) {
                    vector::swap(vec, j, j+1);
                };
                j = j + 1;
            };
            i = i + 1;
        };
    }

    #[test_only]
    fun price_level_orders_to_str(orders: vector<PriceLevelOrder>): string::String {
        let i = 0;
        let size = vector::length(&orders);
        let out = vector[];
        while (i < size) {
            vector::push_back(&mut out, price_level_order_to_str(vector::remove(&mut orders, 0)));
            i = i + 1;
        };
        ftu::join(out, b" ")
    }

    #[test_only]
    fun price_level_order_to_str(order: PriceLevelOrder): string::String {
        let out = s(b"(");
        string::append(&mut out, ftu::u32_to_string(order.id));
        string::append(&mut out, s(b", "));
        string::append(&mut out, ftu::pretty_print_fp(order.qty, DECIMAL_PLACES));
        string::append(&mut out, s(b")"));
        out
    }

    #[test_only]
    fun assert_buy_price_store_qtys<I, Q>(
        marketAddr: address,
        expectedTree: vector<string::String>,
        expectedCache: vector<string::String>,
    ) acquires MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree {
        assert_price_store_qtys<I, Q>(marketAddr, SIDE_BUY, expectedCache, expectedTree)
    }

    #[test_only]
    fun assert_sell_price_store_qtys<I, Q>(
        marketAddr: address,
        expectedTree: vector<string::String>,
        expectedCache: vector<string::String>,
    ) acquires MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree {
        assert_price_store_qtys<I, Q>(marketAddr, SIDE_SELL, expectedCache, expectedTree)
    }

    #[test_only]
    fun assert_price_store_qtys<I, Q>(
        marketAddr: address,
        side: u8,
        expectedCache: vector<string::String>,
        expectedTree: vector<string::String>,
    ) acquires MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree {
        let (tree, cache) = if (side == SIDE_BUY) {
            (&borrow_global<MarketBuyTree<I, Q>>(marketAddr).tree, &borrow_global<MarketBuyCache<I, Q>>(marketAddr).cache)
        } else {
            (&borrow_global<MarketSellTree<I, Q>>(marketAddr).tree, &borrow_global<MarketSellCache<I, Q>>(marketAddr).cache)
        };

        if (market_tree_to_str(tree, side, false) != expectedTree) {
            debug::print(&s(b"Actual Market Tree Qtys"));
            print_market_tree(tree, side, false);
            abort 0
        };
        if (market_cache_to_str(cache, false) != expectedCache) {
            debug::print(&s(b"Actual Market Cache Qtys"));
            print_market_cache(cache, false);
            abort 0
        };
    }

    #[test_only]
    fun print_market_tree(tree: &Tree<PriceStoreElem>, side: u8, priceLevelIDs: bool) {
        debug::print(&market_tree_to_str(tree, side, priceLevelIDs));
    }

    #[test_only]
    fun market_tree_to_str(tree: &Tree<PriceStoreElem>, side: u8, priceLevelIDs: bool): vector<string::String> {
        let out = vector[];
        let it = tree_iterate(tree, if (side == SIDE_BUY) {
            INCREASING_ITERATOR
        } else {
           DECREASING_ITERATOR
        });
        while (it.pos.nodeID != 0) {
            let (price, elem) = tree_get_next(tree, &mut it);
            vector::push_back(&mut out, price_store_elem_to_str(price, elem, priceLevelIDs));
        };
        out
    }

    #[test_only]
    fun market_tree_qtys_to_str(tree: &Tree<PriceStoreElem>, side: u8): vector<string::String> {
        market_tree_to_str(tree, side, false)
    }

    #[test_only]
    fun assert_market_tree(tree: &Tree<PriceStoreElem>, side: u8, expected: vector<string::String>) {
        if (market_tree_to_str(tree, side, true) != expected) {
            debug::print(&s(b"Actual Market Tree"));
            print_market_tree(tree, side, true);
            abort 0
        }
    }

    #[test_only]
    fun assert_market_tree_qtys(tree: &Tree<PriceStoreElem>, side: u8, expected: vector<string::String>) {
        if (market_tree_to_str(tree, side, false) != expected) {
            debug::print(&s(b"Actual Market Tree Qtys"));
            print_market_tree(tree, side, false);
            abort 0
        }
    }

    #[test_only]
    fun print_market_cache(cache: &Cache<PriceStoreElem>, priceLevelIDs: bool) {
        debug::print(&market_cache_to_str(cache, priceLevelIDs));
    }

    #[test_only]
    fun market_cache_to_str(cache: &Cache<PriceStoreElem>, priceLevelIDs: bool): vector<string::String> {
        let out = vector[];
        let i = 0;
        let size = vector::length(&cache.list);
        while (i < size) {
            let elem = vector::borrow(&cache.list, i);
            vector::push_back(&mut out, price_store_elem_to_str(elem.key, &elem.value, priceLevelIDs));
            i = i + 1;
        };
        out
    }

    #[test_only]
    fun assert_market_cache(cache: &Cache<PriceStoreElem>, expected: vector<string::String>) {
        if (market_cache_to_str(cache, true) != expected) {
            debug::print(&s(b"Actual Cache:"));
            print_market_cache(cache, true);
            abort 0
        };
    }

    #[test_only]
    fun assert_market_cache_qtys(cache: &Cache<PriceStoreElem>, expected: vector<string::String>) {
        if (market_cache_to_str(cache, false) != expected) {
            debug::print(&s(b"Actual Cache:"));
            print_market_cache(cache, false);
            abort 0
        };
    }

    #[test_only]
    fun price_store_elem_to_str(price: u64, elem: &PriceStoreElem, priceLevelID: bool): string::String {
        let out = s(b"(");
        string::append(&mut out, ftu::pretty_print_fp(price, DECIMAL_PLACES));
        if (priceLevelID) {
            string::append_utf8(&mut out, b" priceLevelID: ");
            string::append(&mut out, ftu::u16_to_string(elem.priceLevelID));
            string::append_utf8(&mut out, b",");
        };
        string::append_utf8(&mut out, b" qty: ");
        string::append(&mut out, ftu::pretty_print_fp(elem.qty, DECIMAL_PLACES));
        string::append_utf8(&mut out, b", crankQty: ");
        string::append(&mut out, ftu::pretty_print_fp(elem.makerCrankPendingQty, DECIMAL_PLACES));
        string::append_utf8(&mut out, b")");
        out
    }

    #[test_only]
    fun assert_instrument_balance<I, Q>(book: &Orderbook<I, Q>, user: &signer, expected: u64) {
        let userAccIdentifier = platform::account_identifier_for_test(user);
        let acc = table::borrow(&book.marketAccounts, account_key_from_identifier(userAccIdentifier));
        assert!(coin::value(&acc.instrumentBalance) == expected, 0);
    }

    #[test_only]
    fun assert_quote_balance<I, Q>(book: &Orderbook<I, Q>, user: &signer, expected: u64) {
        let userAccIdentifier = platform::account_identifier_for_test(user);
        let acc = table::borrow(&book.marketAccounts, account_key_from_identifier(userAccIdentifier));
        assert!(coin::value(&acc.quoteBalance) == expected, 0);
    }

    #[test_only]
    fun assert_order_collateral<I, Q>(book: &Orderbook<I, Q>, orderID: u32, buy: u64, sell: u64) {
        let order = table::borrow(&book.ordersTable.objects, orderID);
        if (coin::value(&order.buyCollateral) != utils::fp_convert(buy, coin::decimals<Q>(), FP_NO_PRECISION_LOSS)) {
            debug::print(&s(b"Actual buy collateral (in Q coin decimals)"));
            debug::print(&coin::value(&order.buyCollateral));
            abort 0
        };
        if (coin::value(&order.sellCollateral) != utils::fp_convert(sell, coin::decimals<I>(), FP_NO_PRECISION_LOSS)) {
            debug::print(&s(b"Actual sell collateral (in I coin decimals)"));
            debug::print(&coin::value(&order.sellCollateral));
            abort 0
        };
    }

    #[test_only]
    fun assert_order_qtys<I, Q>(
        marketAddr: address,
        orderID: u32,
        unfilledQty: u64,
        takerCrankPendingQty: u64,
        makerCrankPendingQty: u64,
    ) acquires Orderbook, MarketSellCache, MarketBuyCache, MarketSellTree, MarketBuyTree {
        let book = borrow_global<Orderbook<I, Q>>(marketAddr);
        let order = table::borrow(&book.ordersTable.objects, orderID);
        if (order.metadata.unfilledQty != unfilledQty) {
            debug::print(&s(b"Actual unfilled qty"));
            debug::print(&order.metadata.unfilledQty);
            abort 0
        };
        if (order.metadata.takerCrankPendingQty != takerCrankPendingQty) {
            debug::print(&s(b"Actual taker crank pending qty"));
            debug::print(&order.metadata.takerCrankPendingQty);
            abort 0
        };
        let orderMakerQty = if (order.priceLevelID == 0) {
            0
        } else {
            let side = order.metadata.side;
            let price = order.metadata.price;
            let priceLevelMakerQty: u64 = if (is_price_store_elem_in_cache(&book.summary, side, price)) {
                let cache = if (side == SIDE_BUY) {
                    &borrow_global<MarketBuyCache<I, Q>>(marketAddr).cache
                } else {
                    &borrow_global<MarketSellCache<I, Q>>(marketAddr).cache
                };
                let res = cache_find(cache, price);
                assert!(vector::length(&res) > 0, 0);
                let idx = *vector::borrow(&res, 0);
                let node = vector::borrow(&cache.list, idx);
                node.value.makerCrankPendingQty
            } else {
                let tree = if (side == SIDE_BUY) {
                    &mut borrow_global_mut<MarketBuyTree<I, Q>>(marketAddr).tree
                } else {
                    &mut borrow_global_mut<MarketSellTree<I, Q>>(marketAddr).tree
                };
                let pos = tree_find(tree, price);
                assert!(pos.nodeID != 0, 0);
                let (_, elem) = tree_get_mut(tree, &pos);
                elem.makerCrankPendingQty
            };
            let priceLevel = table::borrow(&book.priceLevelsTable.objects, order.priceLevelID);
            let it = list_iterate(&priceLevel.orders);
            let orderMakerQty = 0;
            while (it.nodeID != 0) {
                let priceLevelOrder = list_get_next(&priceLevel.orders, &mut it);
                if (priceLevelOrder.id == orderID) {
                    orderMakerQty = if (priceLevelMakerQty > priceLevelOrder.qty) {
                        priceLevelOrder.qty
                    } else {
                        priceLevelMakerQty
                    };
                    break
                };
                priceLevelMakerQty = if (priceLevelMakerQty > priceLevelOrder.qty) {
                    priceLevelMakerQty - priceLevelOrder.qty
                } else {
                    0
                };
            };
            orderMakerQty
        };
        if (orderMakerQty != makerCrankPendingQty) {
            debug::print(&s(b"Actual maker crank pending qty"));
            debug::print(&orderMakerQty);
            abort 0
        };
    }

    #[test_only]
    fun assert_order_unused<I, Q>(book: &Orderbook<I, Q>, orderID: u32) {
        let order = table::borrow(&book.ordersTable.objects, orderID);
        assert!(order.priceLevelID == 0, 0);
        assert!(order.metadata.ownerAddress == @0, 0);
        let currOrderID = book.ordersTable.unusedStack;
        let found = false;
        while (currOrderID != 0) {
            if (currOrderID == orderID) {
                found = true;
                break
            };
            let currOrder = table::borrow(&book.ordersTable.objects, currOrderID);
            currOrderID = currOrder.next;
        };
        assert!(found, 0);
    }

    #[test_only]
    fun assert_order_used<I, Q>(book: &Orderbook<I, Q>, orderID: u32) {
        let order = table::borrow(&book.ordersTable.objects, orderID);
        assert!(order.next == 0, 0);
        assert!(order.metadata.ownerAddress != @0, 0);
        let currNodeID = book.ordersTable.unusedStack;
        let found = false;
        while (currNodeID != 0) {
            if (currNodeID == orderID) {
                found = true;
                break
            };
            currNodeID = order.next;
        };
        assert!(!found, 0);
    }

    #[test_only]
    fun assert_account_balances<I, Q>(
        book: &Orderbook<I, Q>,
        accountKey: MarketAccountKey,
        instrumentBalance: u64,
        quoteBalance: u64,
    ) {
        let account = table::borrow(&book.marketAccounts, accountKey);
        if (coin::value(&account.instrumentBalance) != utils::fp_convert(instrumentBalance, coin::decimals<I>(), FP_NO_PRECISION_LOSS)) {
            debug::print(&s(b"Actual instrument balance (in I coin decimals)"));
            debug::print(&coin::value(&account.instrumentBalance));
            abort 0
        };
        if (coin::value(&account.quoteBalance) != utils::fp_convert(quoteBalance, coin::decimals<Q>(), FP_NO_PRECISION_LOSS)) {
            debug::print(&s(b"Actual quote balance (in Q coin decimals)"));
            debug::print(&coin::value(&account.quoteBalance));
            abort 0
        };
    }

    #[test_only]
    fun add_user_limit_order<I, Q>(user: &signer, side: u8, behaviour: u8, price: u64, qty: u64): u32
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        let accountKey = MarketAccountKey {
            protocolAddress: @ferum,
            userAddress: address_of(user),
        };
        add_order<I, Q>(user, accountKey, side, behaviour, price, qty, 0, 0)
    }

    #[test_only]
    fun add_user_market_order<I, Q>(user: &signer, side: u8, behaviour: u8, qty: u64, maxBuyCollateral: u64): u32
        acquires FerumInfo, Orderbook, MarketBuyCache, MarketBuyTree, MarketSellCache, MarketSellTree, EventQueue, IndexingEventHandles
    {
        let accountKey = MarketAccountKey {
            protocolAddress: @ferum,
            userAddress: address_of(user),
        };
        add_order<I, Q>(user, accountKey, side, behaviour, 0, qty, 0, maxBuyCollateral)
    }

    #[test_only]
    fun setup_ferum_test<I, Q>(aptos: &signer, ferum: &signer, user: &signer, maxCacheSize: u8): address
        acquires FerumInfo, Orderbook, IndexingEventHandles, MarketSellCache, MarketSellTree, MarketBuyCache, MarketBuyTree, EventQueue
    {
        timestamp::set_time_has_started_for_testing(aptos);
        token::init_fe(ferum);
        account::create_account_for_test(address_of(ferum));
        account::create_account_for_test(address_of(user));
        create_fake_coins(ferum, 8);
        deposit_fake_coins(ferum, 10000000000, user);
        init_ferum(ferum);
        new_fee_type_entry(ferum, s(b"test"), 0, 0, 0);
        init_market_entry<I, Q>(ferum, 4, 4, maxCacheSize, s(b"test"));
        let userIdentifier = platform::account_identifier_for_test(user);
        let accountKey = open_market_account<I, Q>(user, vector[userIdentifier]);
        deposit_to_market_account<I, Q>(user, accountKey, 1000000000000, 1000000000000);
        let marketAddr = get_market_addr<I, Q>();
        // Pre-emptive borrows so test signatures are consistent.
        borrow_global<IndexingEventHandles<I, Q>>(marketAddr);
        borrow_global<MarketSellCache<I, Q>>(marketAddr);
        borrow_global<MarketSellTree<I, Q>>(marketAddr);
        borrow_global<MarketBuyCache<I, Q>>(marketAddr);
        borrow_global<MarketBuyTree<I, Q>>(marketAddr);
        borrow_global<EventQueue<I, Q>>(marketAddr);
        marketAddr
    }

    // </editor-fold>

    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="Inlined admin">

    // Structs.
    // Global info object for ferum.
    struct FerumInfo has key {
        // Map of all markets created, keyed by their instrument quote pairs.
        marketMap: table::Table<string::String, address>,
        // Fee structure for all Ferum market types. Each fee structure is associated with a specific key.
        feeStructures: table::Table<string::String, FeeStructure>,
    }

    // Key used to map to a market address. Is first converted to a string using TypeInfo.
    struct MarketKey<phantom I, phantom Q> has key {}

    // Entry functions.
    // All fee values are fixed points with 4 decimal places.
    public entry fun init_ferum(owner: &signer) {
        let ownerAddr = address_of(owner);
        assert!(!exists<FerumInfo>(ownerAddr), ERR_NOT_ALLOWED);
        assert!(ownerAddr == @ferum, ERR_NOT_ALLOWED);

        move_to(owner, FerumInfo {
            marketMap: table::new<string::String, address>(),
            feeStructures: table::new(),
        });
    }

    public entry fun new_fee_type_entry(
        owner: &signer,
        feeType: string::String,
        takerFee: u64,
        makerFee: u64,
        protocolFeeBps: u64,
    ) acquires FerumInfo {
        let ownerAddr = address_of(owner);
        assert_ferum_inited();
        assert!(ownerAddr == @ferum, ERR_NOT_ALLOWED);
        let info = borrow_global_mut<FerumInfo>(@ferum);
        assert!(!table::contains(&info.feeStructures, feeType), ERR_FEE_TYPE_EXISTS);
        table::add(&mut info.feeStructures, feeType, new_fee_tiers_with_defaults(
            takerFee,
            makerFee,
            protocolFeeBps,
        ));
    }

    public entry fun add_protocol_fee_tier_entry(
        owner: &signer,
        feeType: string::String,
        minFerumTokenHoldings: u64,
        fee: u64,
    ) acquires FerumInfo {
        let ownerAddr = address_of(owner);
        assert_ferum_inited();
        assert!(ownerAddr == @ferum, ERR_NOT_ALLOWED);
        let info = borrow_global_mut<FerumInfo>(@ferum);
        assert!(table::contains(&info.feeStructures, feeType), ERR_INVALID_FEE_TYPE);
        let structure = table::borrow_mut(&mut info.feeStructures, feeType);

        set_protocol_fee_tier(structure, minFerumTokenHoldings, fee);
    }

    public entry fun add_user_fee_tier_entry(
        owner: &signer,
        feeType: string::String,
        minFerumTokenHoldings: u64,
        takerFee: u64,
        makerFee: u64,
    ) acquires FerumInfo {
        let ownerAddr = address_of(owner);
        assert_ferum_inited();
        assert!(ownerAddr == @ferum, ERR_NOT_ALLOWED);
        let info = borrow_global_mut<FerumInfo>(@ferum);
        assert!(table::contains(&info.feeStructures, feeType), ERR_INVALID_FEE_TYPE);
        let structure = table::borrow_mut(&mut info.feeStructures, feeType);

        set_user_fee_tier(structure, minFerumTokenHoldings, takerFee, makerFee);
    }

    public entry fun remove_protocol_fee_tier_entry(
        owner: &signer,
        feeType: string::String,
        minFerumTokenHoldings: u64,
    ) acquires FerumInfo {
        let ownerAddr = address_of(owner);
        assert_ferum_inited();
        assert!(ownerAddr == @ferum, ERR_NOT_ALLOWED);
        let info = borrow_global_mut<FerumInfo>(@ferum);
        assert!(table::contains(&info.feeStructures, feeType), ERR_INVALID_FEE_TYPE);
        let structure = table::borrow_mut(&mut info.feeStructures, feeType);

        remove_protocol_fee_tier(structure, minFerumTokenHoldings);
    }

    public entry fun remove_user_fee_tier_entry(
        owner: &signer,
        feeType: string::String,
        minFerumTokenHoldings: u64,
    ) acquires FerumInfo {
        let ownerAddr = address_of(owner);
        assert_ferum_inited();
        assert!(ownerAddr == @ferum, ERR_NOT_ALLOWED);
        let info = borrow_global_mut<FerumInfo>(@ferum);
        assert!(table::contains(&info.feeStructures, feeType), ERR_INVALID_FEE_TYPE);
        let structure = table::borrow_mut(&mut info.feeStructures, feeType);

        remove_user_fee_tier(structure, minFerumTokenHoldings);
    }

    inline fun assert_ferum_inited() {
        assert!(exists<FerumInfo>(@ferum), ERR_NOT_ALLOWED);
        assert!(coin::is_coin_initialized<token::Fe>(), ERR_FE_UNINITED);
    }

    inline fun register_market<I, Q>(marketAddr: address) acquires FerumInfo {
        assert_ferum_inited();
        let info = borrow_global_mut<FerumInfo>(@ferum);
        let key = market_key<I, Q>();
        assert!(!table::contains(&info.marketMap, key), ERR_MARKET_EXISTS);
        let oppositeKey = market_key<Q, I>();
        assert!(!table::contains(&info.marketMap, oppositeKey), ERR_MARKET_EXISTS);
        table::add(&mut info.marketMap, key, marketAddr);
    }

    inline fun get_fee_structure(feeType: string::String): &FeeStructure acquires FerumInfo {
        assert_ferum_inited();
        let info = borrow_global_mut<FerumInfo>(@ferum);
        assert!(table::contains(&info.feeStructures, feeType), ERR_INVALID_FEE_TYPE);
        table::borrow(&mut info.feeStructures, feeType)
    }

    inline fun get_market_addr<I, Q>(): address acquires FerumInfo {
        assert_ferum_inited();
        let info = borrow_global<FerumInfo>(@ferum);
        let key = market_key<I, Q>();
        assert!(table::contains(&info.marketMap, key), ERR_MARKET_NOT_EXISTS);
        *table::borrow(&info.marketMap, key)
    }

    inline fun assert_market_inited<I, Q>() acquires FerumInfo {
        get_market_addr<I, Q>();
    }

    inline fun market_key<I, Q>(): string::String {
        type_info::type_name<MarketKey<I, Q>>()
    }

    // Tests

    #[test(owner = @ferum)]
    fun test_admin_init_ferum(owner: &signer) {
        // Tests that an account can init ferum.
        init_ferum(owner);
    }

    #[test(owner = @0x1)]
    #[expected_failure]
    fun test_admin_init_not_ferum(owner: &signer) {
        // Tests that an account that's not ferum can't init.
        init_ferum(owner);
    }

    #[test(owner = @ferum)]
    fun test_admin_register_market(owner: &signer) acquires FerumInfo {
        // Tests that a market can be registered.
        account::create_account_for_test(address_of(owner));
        token::init_fe(owner);
        init_ferum(owner);
        create_fake_coins(owner, 8);
        register_market<FMA, FMB>(address_of(owner));
        let market_addr = get_market_addr<FMA, FMB>();
        assert!(market_addr == address_of(owner), 0);
    }

    #[test(owner = @ferum)]
    #[expected_failure]
    fun test_admin_register_other_combination(owner: &signer) acquires FerumInfo {
        // Tests that when market<I, Q> is registered, market<Q, I> is not.
        init_ferum(owner);
        create_fake_coins(owner, 8);
        register_market<FMA, FMB>(address_of(owner));
        let market_addr = get_market_addr<FMA, FMB>();
        assert!(market_addr == address_of(owner), 0);
        get_market_addr<FMB, FMA>();
    }

    #[test(owner = @ferum)]
    fun test_admin_fee_types(owner: &signer) acquires FerumInfo {
        token::init_fe(owner);
        init_ferum(owner);
        new_fee_type_entry(owner, s(b"test"), 5000000, 5000000, 5000000000);
        add_user_fee_tier_entry(owner, s(b"test"), 100, 6000000, 0);
        add_protocol_fee_tier_entry(owner, s(b"test"), 100, 6000000000);

        let structure = get_fee_structure(s(b"test"));
        let (taker, maker) = get_user_fee(structure,25);
        assert!(taker == 5000000, 0);
        assert!(maker == 5000000, 0);
        let (taker, maker) = get_user_fee(structure, 125);
        assert!(taker == 6000000, 0);
        assert!(maker == 0000000, 0);
        let protocolFee = get_protocol_fee(structure, 10);
        assert!(protocolFee == 5000000000, 0);
        let protocolFee = get_protocol_fee(structure, 125);
        assert!(protocolFee == 6000000000, 0);
    }

    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="Inlined fees">

    // Differing teirs users can qualify for.
    struct UserFeeTier has store, drop {
        // Percent fee charged to user trading in this fee tier (if the order is a taker).
        // Represented as a 10 decimal fixed point number. For example, 5000000 == 5bps.
        makerFee: u64,
        // Percent fee charged to user trading in this fee tier (if the order is a maker).
        // Represented as a 10 decimal fixed point number. For example, 5000000 == 5bps.
        takerFee: u64,
    }

    // Differing teirs protocols can qualify for.
    struct ProtocolFeeTier has store, drop {
        // Percentage of the user trading fee that is given to protocols that are in this fee tier.
        // Represented as a 10 decimal fixed point number. For example, 500000000 == 5%.
        protocolFee: u64,
    }

    // Tier structure encapsulating different types of tiers.
    struct Tier<T: store + drop> has store, drop {
        // Minimum Fe a protocol needs to hold to qualify for this fee tier.
        // Represented as a fixed point number with the same number of decimal points as the Fe token (8).
        minFerumTokens: u64,
        // Information about this tier.
        value: T,
    }

    // FeeStructure is an object which describes the fee tiers for a given market. The structures are stored in a map
    // on the
    struct FeeStructure has store, drop {
        // List of UserFeeTiers sorted in increasing order.
        userTiers: vector<Tier<UserFeeTier>>,
        // List of ProtocolFeeTiers sorted in increasing order.
        protocolTiers: vector<Tier<ProtocolFeeTier>>,

        // Ferum takes 100% - protocol fee.
    }

    inline fun new_fee_tiers_with_defaults(takerFee: u64, makerFee: u64, protocolFee: u64): FeeStructure {
        let structure = FeeStructure {
            userTiers: vector[
                Tier{
                    minFerumTokens: 0,
                    value: UserFeeTier {
                        makerFee,
                        takerFee,
                    },
                },
            ],
            protocolTiers: vector[
                Tier{
                    minFerumTokens: 0,
                    value: ProtocolFeeTier {
                        protocolFee,
                    },
                },
            ],
        };
        validate_fees(&structure);
        structure
    }

    // Returns the % protocols get from user fees based on the protocol's fee tier.
    inline fun get_protocol_fee(structure: &FeeStructure, tokenHoldingsAmt: u64): u64 {
        let tier = find_tier<ProtocolFeeTier>(&structure.protocolTiers, tokenHoldingsAmt);
        tier.value.protocolFee
    }

    // Returns (taker, maker) fees for users based on the user's token holdings.
    inline fun get_user_fee(structure: &FeeStructure, tokenHoldingsAmt: u64): (u64, u64) {
        let tier = find_tier<UserFeeTier>(&structure.userTiers, tokenHoldingsAmt);
        (tier.value.takerFee, tier.value.makerFee)
    }

    inline fun set_user_fee_tier(
        structure: &mut FeeStructure,
        minFerumTokens: u64,
        takerFee: u64,
        makerFee: u64,
    ) {
        let tier = Tier {
            value: UserFeeTier {
                makerFee,
                takerFee,
            },
            minFerumTokens,
        };
        set_tier<UserFeeTier>(&mut structure.userTiers, tier);
        validate_fees(structure);
    }

    inline fun set_protocol_fee_tier(
        structure: &mut FeeStructure,
        minFerumTokens: u64,
        protocolFee: u64,
    ) {
        let tier = Tier {
            minFerumTokens,
            value: ProtocolFeeTier {
                protocolFee,
            },
        };
        set_tier<ProtocolFeeTier>(&mut structure.protocolTiers, tier);
        validate_fees(structure);
    }

    inline fun remove_user_fee_tier(structure: &mut FeeStructure, minFerumTokens: u64) {
        remove_tier<UserFeeTier>(&mut structure.userTiers, minFerumTokens);
    }

    inline fun remove_protocol_fee_tier(structure: &mut FeeStructure, minFerumTokens: u64) {
        remove_tier<ProtocolFeeTier>(&mut structure.protocolTiers, minFerumTokens);
    }

    inline fun validate_fees(structure: &FeeStructure) {
        let hundred = 10000000000;
        let bip = 1000000;
        let percent = 100000000;

        let protocolFeeCount = vector::length(&structure.protocolTiers);
        let i = 0;
        while (i < protocolFeeCount) {
            let tier = vector::borrow(&structure.protocolTiers, i);
            if (tier.value.protocolFee != 0) {
                assert!(tier.value.protocolFee >= percent, ERR_INVALID_FEE_STRUCTURE);
            };
            assert!(tier.value.protocolFee <= hundred, ERR_INVALID_FEE_STRUCTURE);
            i = i + 1;
        };

        // Assert that user fees don't exceed 100.
        let i = 0;
        let size = vector::length(&structure.userTiers);
        while (i < size) {
            let tier = vector::borrow(&structure.userTiers, i);
            assert!(tier.value.makerFee < hundred, ERR_INVALID_FEE_STRUCTURE);
            if (tier.value.makerFee != 0) {
                assert!(tier.value.makerFee >= bip, ERR_INVALID_FEE_STRUCTURE);
            };
            assert!(tier.value.takerFee < hundred, ERR_INVALID_FEE_STRUCTURE);
            if (tier.value.takerFee != 0) {
                assert!(tier.value.takerFee >= bip, ERR_INVALID_FEE_STRUCTURE);
            };
            i = i + 1;
        };
    }

    // TODO: make inline once bugs are fixed.
    fun set_tier<T: store + drop>(list: &mut vector<Tier<T>>, tier: Tier<T>) {
        let i = 0;
        let size = vector::length(list);
        let tierMinFe = tier.minFerumTokens;
        assert!(size > 0, ERR_INVALID_FEE_STRUCTURE);
        while (i < size) {
            let curr = vector::borrow_mut(list, i);
            if (curr.minFerumTokens == tierMinFe) {
                *curr = tier;
                return
            };
            if (curr.minFerumTokens > tierMinFe) {
                break
            };
            i = i + 1;
        };
        vector::push_back(list, tier);
        while (i < size) {
            vector::swap(list, i, size);
            i = i + 1;
        };
    }

    fun remove_tier<T: store + drop>(list: &mut vector<Tier<T>>, minFerumTokens: u64) {
        let i = 0;
        let size = vector::length(list);
        assert!(size > 0, ERR_INVALID_FEE_STRUCTURE);
        assert!(minFerumTokens != 0, ERR_TIER_NOT_FOUND); // Can't remove default fee tier.
        let found = false;
        while (i < size) {
            let curr = vector::borrow_mut(list, i);
            if (curr.minFerumTokens == minFerumTokens) {
                found = true;
            };
            if (found && i < size-1) {
                vector::swap(list, i, i+1);
            };
            i = i + 1;
        };
        assert!(found, ERR_TIER_NOT_FOUND);
        vector::pop_back(list);
    }

    fun find_tier<T: store + drop>(list: &vector<Tier<T>>, val: u64): &Tier<T> {
        let size = vector::length(list);
        assert!(size > 0, ERR_INVALID_FEE_STRUCTURE);
        let i = 1;
        while (i < size) {
            let curr = vector::borrow(list, i);
            if (curr.minFerumTokens > val) {
                break
            };
            i = i + 1;
        };
        vector::borrow(list, i - 1)
    }

    #[test]
    fun test_fees_protocol_fee_tiers() {
        let structure = new_fee_tiers_with_defaults(5000000, 0, 500000000);
        // Add some protocol tiers.
        set_protocol_fee_tier(
            &mut structure,
            100,
            1000000000,
        );
        set_protocol_fee_tier(
            &mut structure,
            200,
            2000000000,
        );
        set_protocol_fee_tier(
            &mut structure,
            125,
            1600000000,
        );
        set_protocol_fee_tier(
            &mut structure,
            125,
            1500000000,
        );
        set_protocol_fee_tier(
            &mut structure,
            25,
            1600000000,
        );
        remove_protocol_fee_tier(&mut structure, 25);

        let fee = get_protocol_fee(&structure, 0);
        assert!(fee == 500000000, 0);
        let fee = get_protocol_fee(&structure, 50);
        assert!(fee == 500000000, 0);
        let fee = get_protocol_fee(&structure, 130);
        assert!(fee == 1500000000, 0);
        let fee = get_protocol_fee(&structure, 1000);
        assert!(fee == 2000000000, 0);
    }

    #[test]
    fun test_fees_user_fee_tiers() {
        let structure = new_fee_tiers_with_defaults(25000000, 4000000, 500000000);
        // Add some user tiers.
        set_user_fee_tier(
            &mut structure,
            100,
            20000000,
            3000000,
        );
        set_user_fee_tier(
            &mut structure,
            200,
            10000000,
            1000000,
        );
        set_user_fee_tier(
            &mut structure,
            150,
            18000000,
            5000000,
        );
        set_user_fee_tier(
            &mut structure,
            150,
            15000000,
            2000000,
        );
        set_user_fee_tier(
            &mut structure,
            20,
            19000000,
            2000000,
        );
        remove_user_fee_tier(&mut structure, 20);

        let (taker, maker) = get_user_fee(&structure, 0);
        assert!(taker == 25000000, 0);
        assert!(maker == 4000000, 0);
        let (taker, maker) = get_user_fee(&structure, 75);
        assert!(taker == 25000000, 0);
        assert!(maker == 4000000, 0);
        let (taker, maker) = get_user_fee(&structure, 100);
        assert!(taker == 20000000, 0);
        assert!(maker == 3000000, 0);
        let (taker, maker) = get_user_fee(&structure, 125);
        assert!(taker == 20000000, 0);
        assert!(maker == 3000000, 0);
        let (taker, maker) = get_user_fee(&structure, 150);
        assert!(taker == 15000000, 0);
        assert!(maker == 2000000, 0);
        let (taker, maker) = get_user_fee(&structure, 1500);
        assert!(taker == 10000000, 0);
        assert!(maker == 1000000, 0);
    }

    #[test]
    fun test_fees_remove_fee_tier() {
        let tiers = vector[
            Tier {
                minFerumTokens: 100,
                value: UserFeeTier {
                    takerFee: 10,
                    makerFee: 10,
                },
            },
            Tier {
                minFerumTokens: 200,
                value: UserFeeTier {
                    takerFee: 10,
                    makerFee: 10,
                },
            },
            Tier {
                minFerumTokens: 300,
                value: UserFeeTier {
                    takerFee: 10,
                    makerFee: 10,
                },
            },
        ];

        remove_tier(&mut tiers, 100);
        ftu::assert_vector_equal(&tiers, &vector[
            Tier {
                minFerumTokens: 200,
                value: UserFeeTier {
                    takerFee: 10,
                    makerFee: 10,
                },
            },
            Tier {
                minFerumTokens: 300,
                value: UserFeeTier {
                    takerFee: 10,
                    makerFee: 10,
                },
            },
        ]);

        remove_tier(&mut tiers, 300);
        ftu::assert_vector_equal(&tiers, &vector[
            Tier {
                minFerumTokens: 200,
                value: UserFeeTier {
                    takerFee: 10,
                    makerFee: 10,
                },
            },
        ]);

        remove_tier(&mut tiers, 200);
        ftu::assert_vector_equal(&tiers, &vector[]);
    }

    #[test]
    #[expected_failure(abort_code=ERR_TIER_NOT_FOUND)]
    fun test_fees_remove_fee_tier_doesnt_exist() {
        let tiers = vector[
            Tier {
                minFerumTokens: 100,
                value: UserFeeTier {
                    takerFee: 10,
                    makerFee: 10,
                },
            },
            Tier {
                minFerumTokens: 200,
                value: UserFeeTier {
                    takerFee: 10,
                    makerFee: 10,
                },
            },
            Tier {
                minFerumTokens: 300,
                value: UserFeeTier {
                    takerFee: 10,
                    makerFee: 10,
                },
            },
        ];

        remove_tier(&mut tiers, 150);
    }

    #[test]
    #[expected_failure(abort_code=ERR_INVALID_FEE_STRUCTURE)]
    fun test_fees_invalid_default_protocol_fee_max() {
        new_fee_tiers_with_defaults(0, 0, 20000000000);
    }

    #[test]
    #[expected_failure(abort_code=ERR_INVALID_FEE_STRUCTURE)]
    fun test_fees_invalid_default_protocol_fee_min() {
        new_fee_tiers_with_defaults(0, 0, 1000000);
    }

    #[test]
    #[expected_failure(abort_code=ERR_INVALID_FEE_STRUCTURE)]
    fun test_fees_invalid_default_user_taker_fee_max() {
        new_fee_tiers_with_defaults(20000000000, 0, 0);
    }

    #[test]
    #[expected_failure(abort_code=ERR_INVALID_FEE_STRUCTURE)]
    fun test_fees_invalid_default_user_maker_fee_max() {
        new_fee_tiers_with_defaults(0, 20000000000, 0);
    }

    #[test]
    #[expected_failure(abort_code=ERR_INVALID_FEE_STRUCTURE)]
    fun test_fees_invalid_default_user_taker_fee_min() {
        new_fee_tiers_with_defaults(100000, 0, 0);
    }

    #[test]
    #[expected_failure(abort_code=ERR_INVALID_FEE_STRUCTURE)]
    fun test_fees_invalid_default_user_maker_fee_min() {
        new_fee_tiers_with_defaults(0, 100000, 0);
    }

    #[test]
    #[expected_failure(abort_code=ERR_INVALID_FEE_STRUCTURE)]
    fun test_fees_invalid_tier_user_fees() {
        let structure = new_fee_tiers_with_defaults(0, 0, 0);
        set_user_fee_tier(
            &mut structure,
            100,
            1000,
            0,
        );
    }

    #[test]
    #[expected_failure(abort_code=ERR_INVALID_FEE_STRUCTURE)]
    fun test_fees_invalid_tier_protocol_fees() {
        let structure = new_fee_tiers_with_defaults(0, 0, 0);
        set_protocol_fee_tier(
            &mut structure,
            100,
            1000,
        );
    }

    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="Inlined Cache">

    struct CacheNode<T: store> has store, drop {
        key: u64,
        value: T,
    }

    struct Cache<T: store> has store {
        side: u8, // The cache uses the side enums.
        list: vector<CacheNode<T>>,
    }

    inline fun new_cache<T: drop + store>(type: u8): Cache<T> {
        assert!(type == SIDE_BUY || type == SIDE_SELL, ERR_CACHE_INVALID_TYPE);
        Cache {
            side: type,
            list: vector::empty(),
        }
    }

    // Inserts the price in order into the list cache. If a buy cache, will insert items in increasing order. If a sell
    // cache, will insert in decreasing order.
    inline fun cache_insert<T: drop + store>(cache: &mut Cache<T>, key: u64, value: T) {
        // Find insert index.
        let size = vector::length(&cache.list);
        let i = size;
        if (cache.side == SIDE_BUY) {
            while (i > 0) {
                let p = vector::borrow(&cache.list, i - 1);
                if (key < p.key) {
                    i = i - 1;
                    continue
                };
                assert!(key != p.key, ERR_CACHE_DUPLICATE_ITEM);
                break
            };
        } else {
            while (i > 0) {
                let p = vector::borrow(&cache.list, i - 1);
                if (key > p.key) {
                    i = i - 1;
                    continue
                };
                assert!(key != p.key, ERR_CACHE_DUPLICATE_ITEM);
                break
            };
        };
        // Perform insert.
        vector::push_back(&mut cache.list, CacheNode {key, value});
        while (i < size) {
            vector::swap(&mut cache.list, i, size);
            i = i + 1;
        };
    }

    inline fun cache_remove<T: drop + store>(cache: &mut Cache<T>, key: u64): T {
        let size = vector::length(&cache.list);
        let i = size;
        while (i > 0) {
            let p = vector::borrow(&cache.list, i - 1);
            if (key != p.key) {
                i = i - 1;
                continue
            };
            break
        };
        assert!(i != 0, ERR_CACHE_ITEM_NOT_FOUND);
        i = i - 1;
        while (i < size - 1) {
            vector::swap(&mut cache.list, i, i + 1);
            i = i + 1;
        };
        let CacheNode{
            key: _,
            value,
        } = vector::pop_back(&mut cache.list);
        value
    }

    inline fun cache_remove_idx<T: drop + store>(cache: &mut Cache<T>, idx: u64): T {
        let size = vector::length(&cache.list);
        let i = idx;
        while (i < size - 1) {
            vector::swap(&mut cache.list, i, i + 1);
            i = i + 1;
        };
        let CacheNode{
            key: _,
            value,
        } = vector::pop_back(&mut cache.list);
        value
    }

    // TODO: inline when bug is fixed (https://github.com/aptos-foundation/AIPs/issues/33#issuecomment-1399213932)
    fun cache_find<T: drop + store>(cache: &Cache<T>, key: u64): vector<u64> {
        let size = vector::length(&cache.list);
        let i = size;
        // Look for index of price in the cache.
        while (i > 0) {
            let p = vector::borrow(&cache.list, i - 1);
            if (key == p.key) {
                return vector[i - 1]
            };
            i = i - 1;
        };
        vector[]
    }

    #[test]
    fun test_cache_insertion_deletion_buy() {
        // Tests that items are inserted / deleted into the list in sorted order.

        let cache = new_cache(SIDE_BUY);
        cache_insert(&mut cache, 5, 10);
        cache_insert(&mut cache, 4, 8);
        cache_insert(&mut cache, 3, 6);
        cache_insert(&mut cache, 2, 4);
        cache_insert(&mut cache, 1, 2);

        assert!(vector::length(&cache.list) == 5, 0);
        ftu::assert_vector_equal(
            &cache.list,
            &vector[
                CacheNode { key: 1, value: 2 },
                CacheNode { key: 2, value: 4 },
                CacheNode { key: 3, value: 6 },
                CacheNode { key: 4, value: 8 },
                CacheNode { key: 5, value: 10 },
            ]
        );

        cache_remove(&mut cache, 4);
        assert!(vector::length(&cache.list) == 4, 0);
        ftu::assert_vector_equal(
            &cache.list,
            &vector[
                CacheNode { key: 1, value: 2 },
                CacheNode { key: 2, value: 4 },
                CacheNode { key: 3, value: 6 },
                CacheNode { key: 5, value: 10 },
            ],
        );

        cache_remove(&mut cache, 5);
        assert!(vector::length(&cache.list) == 3, 0);
        ftu::assert_vector_equal(
            &cache.list,
            &vector[
                CacheNode { key: 1, value: 2 },
                CacheNode { key: 2, value: 4 },
                CacheNode { key: 3, value: 6 },
            ],
        );

        cache_remove(&mut cache, 1);
        assert!(vector::length(&cache.list) == 2, 0);
        ftu::assert_vector_equal(
            &cache.list,
            &vector[
                CacheNode { key: 2, value: 4 },
                CacheNode { key: 3, value: 6 },
            ],
        );

        cache_insert(&mut cache, 100, 200);
        assert!(vector::length(&cache.list) == 3, 0);
        ftu::assert_vector_equal(
            &cache.list,
            &vector[
                CacheNode { key: 2, value: 4 },
                CacheNode { key: 3, value: 6 },
                CacheNode { key: 100, value: 200 },
            ],
        );

        // Remove all from cache.
        cache_remove(&mut cache, 2);
        cache_remove(&mut cache, 3);
        cache_remove(&mut cache, 100);
        assert!(vector::length(&cache.list) == 0, 0);
        ftu::assert_vector_equal(
            &cache.list,
            &vector[],
        );

        cache_insert(&mut cache, 2, 4);
        assert!(vector::length(&cache.list) == 1, 0);
        ftu::assert_vector_equal(
            &cache.list,
            &vector[
                CacheNode { key: 2, value: 4 },
            ],
        );
        cache_remove(&mut cache, 2);
        assert!(vector::length(&cache.list) == 0, 0);
        ftu::assert_vector_equal(
            &cache.list,
            &vector[],
        );

        destroy_cache(cache);
    }

    #[test]
    fun test_cache_insertion_deletion_sell() {
        // Tests that items are inserted / deleted into the list in sorted order.

        let cache = new_cache(SIDE_SELL);
        cache_insert(&mut cache, 5, 10);
        cache_insert(&mut cache, 4, 8);
        cache_insert(&mut cache, 3, 6);
        cache_insert(&mut cache, 2, 4);
        cache_insert(&mut cache, 1, 2);

        assert!(vector::length(&cache.list) == 5, 0);
        ftu::assert_vector_equal(
            &cache.list,
            &vector[
                CacheNode { key: 5, value: 10 },
                CacheNode { key: 4, value: 8 },
                CacheNode { key: 3, value: 6 },
                CacheNode { key: 2, value: 4 },
                CacheNode { key: 1, value: 2 },
            ],
        );

        cache_remove(&mut cache, 4);
        assert!(vector::length(&cache.list) == 4, 0);
        ftu::assert_vector_equal(
            &cache.list,
            &vector[
                CacheNode { key: 5, value: 10 },
                CacheNode { key: 3, value: 6 },
                CacheNode { key: 2, value: 4 },
                CacheNode { key: 1, value: 2 },
            ],
        );

        cache_remove(&mut cache, 5);
        assert!(vector::length(&cache.list) == 3, 0);
        ftu::assert_vector_equal(
            &cache.list,
            &vector[
                CacheNode { key: 3, value: 6 },
                CacheNode { key: 2, value: 4 },
                CacheNode { key: 1, value: 2 },
            ],
        );

        cache_remove(&mut cache, 1);
        assert!(vector::length(&cache.list) == 2, 0);
        ftu::assert_vector_equal(
            &cache.list,
            &vector[
                CacheNode { key: 3, value: 6 },
                CacheNode { key: 2, value: 4 },
            ],
        );

        cache_insert(&mut cache, 100, 200);
        assert!(vector::length(&cache.list) == 3, 0);
        ftu::assert_vector_equal(
            &cache.list,
            &vector[
                CacheNode { key: 100, value: 200 },
                CacheNode { key: 3, value: 6 },
                CacheNode { key: 2, value: 4 },
            ],
        );

        // Remove all from cache.
        cache_remove(&mut cache, 2);
        cache_remove(&mut cache, 3);
        cache_remove(&mut cache, 100);
        assert!(vector::length(&cache.list) == 0, 0);
        ftu::assert_vector_equal(
            &cache.list,
            &vector[],
        );

        cache_insert(&mut cache, 2, 4);
        assert!(vector::length(&cache.list) == 1, 0);
        ftu::assert_vector_equal(
            &cache.list,
            &vector[
                CacheNode { key: 2, value: 4 },
            ],
        );
        cache_remove(&mut cache, 2);
        assert!(vector::length(&cache.list) == 0, 0);
        ftu::assert_vector_equal(
            &cache.list,
            &vector[],
        );

        destroy_cache(cache);
    }

    #[test]
    #[expected_failure(abort_code=ERR_CACHE_DUPLICATE_ITEM)]
    fun test_cache_sell_duplicate() {
        let cache = new_cache(SIDE_SELL);
        cache_insert(&mut cache, 1, 2);
        cache_insert(&mut cache, 1, 4);
        destroy_cache(cache);
    }

    #[test]
    #[expected_failure(abort_code=ERR_CACHE_DUPLICATE_ITEM)]
    fun test_cache_buy_duplicate() {
        let cache = new_cache(SIDE_BUY);
        cache_insert(&mut cache, 1, 6);
        cache_insert(&mut cache, 1, 5);
        destroy_cache(cache);
    }

    #[test]
    #[expected_failure(abort_code=ERR_CACHE_ITEM_NOT_FOUND)]
    fun test_cache_sell_unknown_item() {
        let cache = new_cache(SIDE_SELL);
        cache_insert(&mut cache, 1, 3);
        cache_insert(&mut cache, 2, 8);
        cache_remove(&mut cache, 10);
        destroy_cache(cache);
    }

    #[test]
    #[expected_failure(abort_code=ERR_CACHE_ITEM_NOT_FOUND)]
    fun test_cache_buy_unknown_item() {
        let cache = new_cache(SIDE_BUY);
        cache_insert(&mut cache, 1, 3);
        cache_insert(&mut cache, 2, 9);
        cache_remove(&mut cache, 10);
        destroy_cache(cache);
    }

    #[test_only]
    fun destroy_cache<T: copy + drop + store>(cache: Cache<T>) {
        Cache {
            side: _,
            list: _,
        } = cache;
    }

    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="Inlined NodeList">

    struct ListIterator has drop {
        prevNodeID: u32,
        nodeID: u32,
        nodeIdx: u64,
    }

    struct NodeListNode<T: store> has store {
        data: vector<T>,
        next: u32,
    }

    struct NodeList<T: store> has store {
        nodes: table::Table<u32, NodeListNode<T>>,
        head: u32,
        tail: u32,
        unusedNodeStack: u32,
        nodeSize: u8,
        currNodeKey: u32,
        length: u64,
    }

    inline fun new_list<T: store>(nodeSize: u8): NodeList<T> {
        NodeList<T>{
            nodes: table::new(),
            head: 0,
            tail: 0,
            unusedNodeStack: 0,
            nodeSize,
            currNodeKey: 1,
            length: 0,
        }
    }

    inline fun list_pop<T: store>(list: &mut NodeList<T>): T {
        assert!(list.length > 0, ERR_LIST_EMPTY);
        let nodeID = list.head;
        let node = table::borrow_mut(&mut list.nodes, nodeID);
        // Inlined pop from front.
        let i = 0;
        let size = vector::length(&node.data);
        while (i < size - 1) {
            vector::swap(&mut node.data, i, i + 1);
            i = i + 1;
        };
        let elem = vector::pop_back(&mut node.data);
        if (size == 1) {
            // If node is now empty, push it to the unused node stack.
            list.head = node.next;
            if (list.tail == nodeID) {
                list.tail = 0;
            };
            node.next = list.unusedNodeStack;
            list.unusedNodeStack = nodeID;
        };
        list.length = list.length - 1;
        elem
    }

    // Returns elements from first node of the list in reversed order.
    inline fun list_pop_node_reversed<T: store>(list: &mut NodeList<T>): vector<T> {
        assert!(list.length > 0, ERR_LIST_EMPTY);
        let nodeID = list.head;
        let node = table::borrow_mut(&mut list.nodes, nodeID);
        let out = vector[];
        let i = 0;
        let size = vector::length(&node.data);
        while (i < size) { // Move elements out of node.
            vector::push_back(&mut out, vector::pop_back(&mut node.data));
            i = i + 1;
        };
        // Node is now empty, push it to the unused node stack.
        list.head = node.next;
        if (list.tail == nodeID) {
            list.tail = 0;
        };
        node.next = list.unusedNodeStack;
        list.unusedNodeStack = nodeID;
        list.length = list.length - size;
        out
    }

    // Drops from the front of the list up to count elements.
    inline fun list_drop_from_front<T: store + drop>(list: &mut NodeList<T>, countInput: u64) {
        let count = countInput;
        while (count > 0 && list.head != 0) {
            let nodeID = list.head;
            let node = table::borrow_mut(&mut list.nodes, nodeID);
            let size = vector::length(&node.data);
            if (count >= size) {
                node.data = vector[];
                // Node is now empty, push it to the unused node stack.
                list.head = node.next;
                if (list.tail == nodeID) {
                    list.tail = 0;
                };
                node.next = list.unusedNodeStack;
                list.unusedNodeStack = nodeID;
                list.length = list.length - size;
                count = count - size;
            } else {
                // Otherwise, pop from the front the remaining number of elements.
                let i = 0;
                while (i < size/2) { // Temporarily reverse the elements.
                    vector::swap(&mut node.data, i, size - i - 1);
                    i = i + 1;
                };
                i = 0;
                while (i < count) { // Pop elements to output.
                    vector::pop_back(&mut node.data);
                    i = i + 1;
                };
                size = size - count;
                while (i < size/2) { // Reset elements to correct order.
                    vector::swap(&mut node.data, i, size - i - 1);
                    i = i + 1;
                };
                list.length = list.length - count;
                count = 0;
                // Node remains in the list because it still has elements.
            };
        };
    }

    inline fun list_push<T: store>(list: &mut NodeList<T>, elem: T) {
        let nodeID = list.tail;
        if (nodeID == 0) {
            nodeID = get_or_create_list_node(list);
            list.head = nodeID;
            list.tail = nodeID;
        };
        let node = table::borrow_mut(&mut list.nodes, nodeID);
        let size = vector::length(&node.data);
        let nodeSize = (list.nodeSize as u64);
        if (size >= nodeSize) {
            let oldNodeID = nodeID;
            nodeID = get_or_create_list_node(list);
            let oldNode = table::borrow_mut(&mut list.nodes, oldNodeID); // Reborrow old node.
            oldNode.next = nodeID;
            node = table::borrow_mut(&mut list.nodes, nodeID); // New node.
            list.tail = nodeID;
        };
        list.length = list.length + 1;
        vector::push_back(&mut node.data, elem);
    }

    inline fun list_remove<T: store + drop>(list: &mut NodeList<T>, it: ListIterator) {
        list.length = list.length - 1;
        let i = it.nodeIdx + 1;
        let nodeID = it.nodeID;
        let prevNodeID = it.prevNodeID;
        let node = table::borrow_mut(&mut list.nodes, nodeID);
        let size = vector::length(&node.data);
        while (i < size) {
            vector::swap(&mut node.data, i, i - 1);
            i = i + 1;
        };
        vector::pop_back(&mut node.data);
        size = size - 1;
        // Remove node if it falls below the desired capacity. If there are elements in the node, add them to a
        // sibling node.
        let nextNodeID = node.next;
        let reversedNodeData = vector[];
        if (size == 0 || (size <= (list.nodeSize as u64)/2 && (nextNodeID != 0 || prevNodeID != 0))) {
            // Create a reversed copy of the data in the node.
            if (size > 0) {
                let j = 0;
                while (j < size) {
                    vector::push_back(&mut reversedNodeData, vector::pop_back(&mut node.data));
                    j = j + 1;
                };
            };
            // Remove node and add it to the reuse stack.
            if (list.tail == nodeID) {
                list.tail = prevNodeID;
            };
            if (list.head == nodeID) {
                list.head = nextNodeID;
            };
            node.next = list.unusedNodeStack;
            list.unusedNodeStack = nodeID;
            // Update prev node.
            if (prevNodeID != 0) {
                let prevNode = table::borrow_mut(&mut list.nodes, prevNodeID);
                prevNode.next = nextNodeID;
            };
            // If any elements from the node were removed, add them to a sibling.
            if (size > 0) {
                // Put elements into sibling node.
                if (prevNodeID != 0) {
                    // Merge with the prev node if is exists.
                    let prevNode = table::borrow_mut(&mut list.nodes, prevNodeID);
                    let j = 0;
                    while (j < size) {
                        vector::push_back(&mut prevNode.data, vector::pop_back(&mut reversedNodeData));
                        j = j + 1;
                    };
                } else if (nextNodeID != 0) {
                    // Otherwise, merge with the next node.
                    let nextNode = table::borrow_mut(&mut list.nodes, nextNodeID);
                    // Reverse original node data.
                    let j = 0;
                    let sizeHalf = size/2;
                    while (j < sizeHalf) {
                        vector::swap(&mut reversedNodeData, j, size - j - 1);
                        j = j + 1;
                    };
                    // Reverse next node data.
                    j = 0;
                    let nextNodeSize = vector::length(&nextNode.data);
                    let nextNodeSizeHalf = nextNodeSize/2;
                    while (j < nextNodeSizeHalf) {
                        vector::swap(&mut nextNode.data, j, nextNodeSize - j - 1);
                        j = j + 1;
                    };
                    // Add elements of original node to the next node.
                    j = 0;
                    while (j < size) {
                        vector::push_back(&mut nextNode.data, vector::pop_back(&mut reversedNodeData));
                        j = j + 1;
                    };
                    // Reverse merged data.
                    j = 0;
                    let totalSize = nextNodeSize + size;
                    let totalSizeHalf = totalSize / 2;
                    while (j < totalSizeHalf) {
                        vector::swap(&mut nextNode.data, j, totalSize - j - 1);
                        j = j + 1;
                    };
                };
            };
        };
    }

    inline fun list_iterate<T: store>(list: &NodeList<T>): ListIterator {
        ListIterator {
            prevNodeID: 0,
            nodeID: list.head,
            nodeIdx: 0,
        }
    }

    inline fun list_peek<T: store>(list: &NodeList<T>, it: &ListIterator): &T {
        assert!(it.nodeID != 0, ERR_LIST_ELEM_NOT_FOUND);
        let node = table::borrow(&list.nodes, it.nodeID);
        vector::borrow(&node.data, it.nodeIdx)
    }

    inline fun list_get_next<T: store>(list: &NodeList<T>, it: &mut ListIterator): &T {
        assert!(it.nodeID != 0, ERR_LIST_ELEM_NOT_FOUND);
        let node = table::borrow(&list.nodes, it.nodeID);
        let out = vector::borrow(&node.data, it.nodeIdx);
        it.nodeIdx = it.nodeIdx + 1;
        if (it.nodeIdx >= vector::length(&node.data)) {
            it.nodeIdx = 0;
            it.prevNodeID = it.nodeID;
            it.nodeID = node.next;
        };
        out
    }

    inline fun list_get_next_mut<T: store>(list: &mut NodeList<T>, it: &mut ListIterator): &mut T {
        let out = list_get_mut(list, it);
        list_next(list, it);
        out
    }

    inline fun list_get_mut<T: store>(list: &mut NodeList<T>, it: &ListIterator): &mut T {
        assert!(it.nodeID != 0, ERR_LIST_ELEM_NOT_FOUND);
        let node = table::borrow_mut(&mut list.nodes, it.nodeID);
        vector::borrow_mut(&mut node.data, it.nodeIdx)
    }

    inline fun list_next<T: store>(list: &NodeList<T>, it: &mut ListIterator) {
        let node = table::borrow(&list.nodes, it.nodeID);
        it.nodeIdx = it.nodeIdx + 1;
        if (it.nodeIdx >= vector::length(&node.data)) {
            it.nodeIdx = 0;
            it.prevNodeID = it.nodeID;
            it.nodeID = node.next;
        };
    }

    inline fun get_or_create_list_node<T: store>(list: &mut NodeList<T>): u32 {
        if (list.unusedNodeStack == 0) {
            prealloc_list_nodes(list, 1);
        };
        let nodeID = list.unusedNodeStack;
        let node = table::borrow_mut(&mut list.nodes, nodeID);
        list.unusedNodeStack = node.next;
        node.next = 0;
        nodeID
    }

    inline fun prealloc_list_nodes<T: store>(list: &mut NodeList<T>, count: u8) {
        let i = 0;
        while (i < count) {
            let nodeID = list.currNodeKey;
            list.currNodeKey = list.currNodeKey + 1;
            let node = NodeListNode {
                data: vector[],
                next: list.unusedNodeStack,
            };
            list.unusedNodeStack = nodeID;
            table::add(&mut list.nodes, nodeID, node);
            i = i + 1;
        }
    }

    #[test]
    fun test_list_iterate() {
        let list = new_list<u16>(4);
        list_push(&mut list, 2);
        list_push(&mut list, 1);
        list_push(&mut list, 3);
        list_push(&mut list, 5);
        list_push(&mut list, 6);
        list_push(&mut list, 7);
        list_push(&mut list, 8);
        list_push(&mut list, 10);
        list_push(&mut list, 11);
        list_pop(&mut list);
        assert_list(&list, vector[1, 3, 5, 6, 7, 8, 10, 11]);
        let actual = vector[];
        let it = list_iterate(&list);
        while (it.nodeID != 0) {
            let peek = *list_peek(&list, &it);
            let next = *list_get_next(&list, &mut it);
            assert!(peek == next, 0);
            vector::push_back(&mut actual, next);
        };
        assert!(actual == vector[1, 3, 5, 6, 7, 8, 10, 11], 0);
        destroy_list(list);
    }

    #[test]
    fun test_list_push_pop() {
        let list = new_list<u16>(4);
        list_push(&mut list, 2);
        list_push(&mut list, 1);
        list_push(&mut list, 3);
        assert_list(&list, vector[2, 1, 3]);
        assert!(list_pop(&mut list) == 2, 0);
        assert_list(&list, vector[1, 3]);
        assert!(list_pop(&mut list) == 1, 0);
        assert_list(&list, vector[3]);
        list_push(&mut list, 100);
        list_push(&mut list, 20);
        list_push(&mut list, 1);
        assert_list(&list, vector[3, 100, 20, 1]);
        assert!(list_pop(&mut list) == 3, 0);
        assert!(list_pop(&mut list) == 100, 0);
        assert!(list_pop(&mut list) == 20, 0);
        assert!(list_pop(&mut list) == 1, 0);
        assert_list(&list, vector[]);
        destroy_list(list);
    }

    #[test]
    fun test_list_pop_node_reversed() {
        let list = new_list<u16>(4);
        list_push(&mut list, 2);
        list_push(&mut list, 1);
        list_push(&mut list, 3);
        assert_list(&list, vector[2, 1, 3]);
        assert!(list_pop_node_reversed(&mut list) == vector[3, 1, 2], 0);
        assert_list(&list, vector[]);
        list_push(&mut list, 100);
        list_push(&mut list, 20);
        list_push(&mut list, 1);
        list_push(&mut list, 20);
        list_push(&mut list, 21);
        assert_list(&list, vector[100, 20, 1, 20, 21]);
        assert!(list_pop_node_reversed(&mut list) == vector[20, 1, 20, 100], 0);
        assert_list(&list, vector[21]);
        destroy_list(list);
    }

    #[test]
    fun test_list_drop_from_front() {
        let list = new_list<u16>(4);
        list_push(&mut list, 2);
        list_push(&mut list, 1);
        list_push(&mut list, 3);
        assert_list(&list, vector[2, 1, 3]);
        list_drop_from_front(&mut list, 0);
        assert_list(&list, vector[2, 1, 3]);
        list_drop_from_front(&mut list, 2);
        assert_list(&list, vector[3]);
        list_drop_from_front(&mut list, 1);
        assert_list(&list, vector[]);
        list_push(&mut list, 100);
        list_push(&mut list, 20);
        list_push(&mut list, 1);
        list_push(&mut list, 20);
        list_push(&mut list, 21);
        assert_list(&list, vector[100, 20, 1, 20, 21]);
        list_drop_from_front(&mut list, 100);
        assert_list(&list, vector[]);
        destroy_list(list);
    }

    #[test]
    fun test_list_remove_from_middle_node_merge_prev() {
        let list = list_from_vector(5,
            vector[
                1, 2, 3, 4, 5,
                6, 7, 8, 9, 10,
                11, 12, 13, 14, 15,
                16, 17, 18, 19, 20,
            ],
        );
        list_remove_elem(&mut list, 6);
        list_remove_elem(&mut list, 7);
        list_remove_elem(&mut list, 8);
        assert_list(&list, vector[1, 2, 3, 4, 5, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20]);
        assert_list_nodes(&list, vector[
            vector[1, 2, 3, 4, 5, 9, 10],
            vector[11, 12, 13, 14, 15],
            vector[16, 17, 18, 19, 20],
        ]);
        destroy_list(list);
    }

    #[test]
    fun test_list_remove_from_end_node_merge_prev() {
        let list = list_from_vector(5,
            vector[
                1, 2, 3, 4, 5,
                6, 7, 8, 9, 10,
                11, 12, 13, 14, 15,
                16, 17, 18, 19, 20,
            ],
        );
        let oldTail = list.tail;
        let oldHead = list.head;
        list_remove_elem(&mut list, 18);
        list_remove_elem(&mut list, 20);
        list_remove_elem(&mut list, 19);
        assert_list(&list, vector[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17]);
        assert_list_nodes(&list, vector[
            vector[1, 2, 3, 4, 5],
            vector[6, 7, 8, 9, 10],
            vector[11, 12, 13, 14, 15, 16, 17],
        ]);
        assert!(oldTail != list.tail, 0);
        assert!(oldHead == list.head, 0);
        destroy_list(list);
    }

    #[test]
    fun test_list_remove_node_merge_next() {
        let list = list_from_vector(5,
            vector[
                1, 2, 3, 4, 5,
                6, 7, 8, 9, 10,
                11, 12, 13, 14, 15,
                16, 17, 18, 19, 20,
            ],
        );
        let oldTail = list.tail;
        let oldHead = list.head;
        list_remove_elem(&mut list, 1);
        list_remove_elem(&mut list, 3);
        list_remove_elem(&mut list, 4);
        assert_list(&list, vector[2, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20]);
        assert_list_nodes(&list, vector[
            vector[2, 5, 6, 7, 8, 9, 10],
            vector[11, 12, 13, 14, 15],
            vector[16, 17, 18, 19, 20],
        ]);
        assert!(oldTail == list.tail, 0);
        assert!(oldHead != list.head, 0);
        destroy_list(list);
    }

    #[test]
    fun test_list_nodes() {
        let list = new_list<u16>(2);
        list_push(&mut list, 3);
        list_push(&mut list, 100);
        {
            assert!(list.head == 1, 0);
            let head = table::borrow(&list.nodes, list.head);
            ftu::assert_vector_equal(&head.data, &vector[3, 100]);
            assert!(head.next == 0, 0);
            assert!(list.tail == 1, 0);
        };
        list_push(&mut list, 20);
        {
            let head = table::borrow(&list.nodes, list.head);
            assert!(head.next == 2, 0);
            let secondNode = table::borrow(&list.nodes, 2);
            ftu::assert_vector_equal(&secondNode.data, &vector[20]);
            assert!(secondNode.next == 0, 0);
            assert!(list.tail == 2, 0);
        };
        list_push(&mut list, 1);
        {
            let head = table::borrow(&list.nodes, list.head);
            assert!(head.next == 2, 0);
            let secondNode = table::borrow(&list.nodes, 2);
            ftu::assert_vector_equal(&secondNode.data, &vector[20, 1]);
            assert!(secondNode.next == 0, 0);
            assert!(list.tail == 2, 0);
        };
        list_push(&mut list, 5);
        {
            let head = table::borrow(&list.nodes, list.head);
            assert!(head.next == 2, 0);
            let secondNode = table::borrow(&list.nodes, 2);
            assert!(secondNode.next == 3, 0);
            let thirdNode = table::borrow(&list.nodes, 3);
            ftu::assert_vector_equal(&thirdNode.data, &vector[5]);
            assert!(thirdNode.next == 0, 0);
            assert!(list.tail == 3, 0);
        };
        assert_list(&list, vector[3, 100, 20, 1, 5]);
        assert!(list.currNodeKey == 4, 0);

        assert!(list_pop(&mut list) == 3, 0);
        {
            assert!(list.head == 1, 0);
            let head = table::borrow(&list.nodes, list.head);
            assert!(head.next == 2, 0);
            ftu::assert_vector_equal(&head.data, &vector[100]);
            let secondNode = table::borrow(&list.nodes, 2);
            assert!(secondNode.next == 3, 0);
            ftu::assert_vector_equal(&secondNode.data, &vector[20, 1]);
            let thirdNode = table::borrow(&list.nodes, 3);
            assert!(thirdNode.next == 0, 0);
            ftu::assert_vector_equal(&thirdNode.data, &vector[5]);
            assert!(list.tail == 3, 0);
        };
        assert!(list_pop(&mut list) == 100, 0);
        {
            assert!(list.head == 2, 0);
            let head = table::borrow(&list.nodes, list.head);
            assert!(head.next == 3, 0);
            ftu::assert_vector_equal(&head.data, &vector[20, 1]);
            let secondNode = table::borrow(&list.nodes, 3);
            assert!(secondNode.next == 0, 0);
            ftu::assert_vector_equal(&secondNode.data, &vector[5]);
            assert!(list.tail == 3, 0);
        };
        assert!(list_pop(&mut list) == 20, 0);
        {
            assert!(list.head == 2, 0);
            let head = table::borrow(&list.nodes, list.head);
            assert!(head.next == 3, 0);
            ftu::assert_vector_equal(&head.data, &vector[1]);
            let secondNode = table::borrow(&list.nodes, 3);
            assert!(secondNode.next == 0, 0);
            ftu::assert_vector_equal(&secondNode.data, &vector[5]);
            assert!(list.tail == 3, 0);
        };
        assert!(list_pop(&mut list) == 1, 0);
        {
            assert!(list.head == 3, 0);
            let head = table::borrow(&list.nodes, list.head);
            assert!(head.next == 0, 0);
            ftu::assert_vector_equal(&head.data, &vector[5]);
            assert!(list.tail == 3, 0);
        };
        assert!(list_pop(&mut list) == 5, 0);
        {
            assert!(list.head == 0, 0);
            assert!(list.tail == 0, 0);
        };

        destroy_list(list);
    }

    #[test]
    fun test_list_prealloc_nodes() {
        let list = new_list<u16>(2);
        prealloc_list_nodes(&mut list, 3);
        assert!(list.currNodeKey == 4, 0);
        list_push(&mut list, 1);
        list_push(&mut list, 2);
        list_push(&mut list, 3);
        assert!(list.currNodeKey == 4, 0);
        destroy_list(list);
    }

    #[test]
    fun test_list_push_duplicate_elems() {
        let list = new_list<u16>(2);
        list_push(&mut list, 100);
        list_push(&mut list, 100);
        list_push(&mut list, 100);
        assert_list(&list, vector[100, 100, 100]);
        assert!(list_pop(&mut list) == 100, 0);
        assert!(list_pop(&mut list) == 100, 0);
        assert!(list_pop(&mut list) == 100, 0);
        assert_list(&list, vector[]);
        destroy_list(list);
    }

    #[test_only]
    fun destroy_list<T: store + drop>(list: NodeList<T>) {
        let NodeList<T>{
            nodes,
            head: _,
            tail: _,
            unusedNodeStack: _,
            nodeSize: _,
            currNodeKey: _,
            length: _,
        } = list;
        table::drop_unchecked(nodes);
    }

    #[test_only]
    fun assert_list<T: store + copy + drop>(list: &NodeList<T>, expected: vector<T>) {
        ftu::assert_vector_equal(&list_to_vector(list), &expected);
        assert!(list.length == vector::length(&expected), 0);
    }

    #[test_only]
    fun assert_list_nodes<T: store + copy + drop>(list: &NodeList<T>, expected: vector<vector<T>>) {
        let currNodeID = list.head;
        let i = 0;
        while (currNodeID != 0) {
            let node = table::borrow(&list.nodes, currNodeID);
            ftu::assert_vector_equal(&node.data, vector::borrow(&expected, i));
            currNodeID = node.next;
            i = i + 1;
        };
        assert!(i == vector::length(&expected), 0);
    }

    #[test_only]
    fun list_to_vector<T: store + copy + drop>(list: &NodeList<T>): vector<T> {
        let out = vector[];
        let it = list_iterate(list);
        while (it.nodeID != 0) {
            vector::push_back(&mut out, *list_get_next(list, &mut it));
        };
        out
    }

    #[test_only]
    fun list_remove_elem<T: store + copy + drop>(list: &mut NodeList<T>, target: T) {
        let it = list_iterate(list);
        while (it.nodeID != 0) {
            let elem = *list_peek(list, &it);
            if (elem == target) {
                list_remove(list, it);
                return
            };
            list_next(list, &mut it);
        };
    }

    #[test_only]
    fun list_from_vector<T: drop + store + copy>(capacity: u8, vec: vector<T>): NodeList<T> {
        let list = new_list(capacity);
        let i = 0;
        let size = vector::length(&vec);
        while (i < size) {
            let elem = *vector::borrow(&vec, i);
            list_push(&mut list, elem);
            i = i + 1;
        };
        list
    }

    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="Inlined B-Tree">

    // Structs

    struct TreeIterator has drop {
        pos: TreePosition,
        type: u8,
    }

    // The position of an element in the tree. Has the store ability so that it can be stored on the Order object.
    struct TreePosition has drop, store, copy {
        nodeID: u16,
        idx: u64,
    }

    // Intermediate struct used to help with deletions.
    struct TreePathInfo<T> has drop {
        // The id of the node.
        nodeID: u16,
        // The number of elements at the node.
        size: u64,
        // Each PathInfo describes the path taken down to a leaf. Each path will have up to two corresponding
        // elements in the parent node:
        //
        //  ( [ ] [A] [B] [ ] )
        //   |   |   |   |   |
        //
        // rightElemIdx is the index of the element to the right of the child encountered on the path.
        // Can exceed the size of the elements. In this case, indicates that the last child was taken.
        //
        // For a leaf node, will indicate the index of the element being searched for.
        rightElemIdx: u64,
        // The key of the element to the right of the child encountered on the path.
        // Children keys are all in (leftElemKey, rightElemKey].
        rightElemKey: u64,
        // The value of the element to the right of the child encountered on the path.
        rightElemValue: vector<T>,
        // The key of the element to the left of the child encountered on the path.
        // Children keys are all in (leftElemKey, rightElemKey].
        leftElemKey: u64,
        // The value of the element to the left of the child encountered on the path.
        leftElemValue: vector<T>,
        // ID of immediate left sibling, if any.
        leftSibling: u16,
        // ID of immediate right sibling, if any.
        rightSibling: u16,
    }

    struct TreeElem<T: copy + store + drop> has store, drop, copy {
        key: u64,
        value: vector<T>,
        child: u16,
    }

    struct TreeNode<T: copy + store + drop> has store, drop {
        elements: vector<TreeElem<T>>,
        firstChild: u16,
        next: u16,
        prev: u16,
    }

    // Represents a B+ tree structure of even degree m (>= 4). The tree has the following invariants:
    //   1. All nodes have a max of m elements
    //   2. Leaf nodes have a at least floor(m/2) elements
    //   3. Internal nodes (non root) have at least floor(m/2) - 1 elements
    //   4. The root node has at least 1 element
    //   5. Each node has 1 more child than the number of elements in that node
    //   6. Children in a node are stored in sorted order and are within the folling ranges (note inclusivity of ranges):
    //       - 1st child of node has elements in (-inf, node.elems[0]]
    //       - 3rd child of node has elements in (node.elems[1], node.elems[2]]
    //       - mth child of node has elements in (node.elems[m-1], inf)
    //   7. All data is stored in leaf nodes and each leaf node is connected to one another in order
    struct Tree<T: copy + store + drop> has store {
        root: u16,
        nodes: table::Table<u16, TreeNode<T>>,
        treeSize: u64,
        m: u64,
        currNodeID: u16,
        unusedNodeStack: u16,
        min: u16,
        max: u16,
    }

    inline fun new_tree<T: copy + store + drop>(m: u64): Tree<T> {
        assert!(m >= 4, ERR_TREE_INVALID_TREE_DEGREE);
        assert!(m % 2 == 0, ERR_TREE_INVALID_TREE_DEGREE);
        Tree {
            root: 0,
            treeSize: 0,
            nodes: table::new(),
            m,
            currNodeID: 1,
            unusedNodeStack: 0,
            min: 0,
            max: 0,
        }
    }

    inline fun prealloc_tree_nodes<T: copy + store + drop>(tree: &mut Tree<T>, count: u8) {
        let i = 0;
        while (i < count) {
            let node = TreeNode {
                elements: vector::empty(),
                prev: 0,
                next: 0,
                firstChild: 0,
            };
            let nodeID = tree.currNodeID;
            node.next = tree.unusedNodeStack;
            tree.currNodeID = tree.currNodeID + 1;
            tree.unusedNodeStack = nodeID;
            table::add(&mut tree.nodes, nodeID, node);
            i = i + 1;
        }
    }

    // Find the position of the given key in the tree.
    // TODO: inline when bug is fixed (https://github.com/aptos-foundation/AIPs/issues/33#issuecomment-1399213932)
    fun tree_find<T: copy + store + drop>(tree: &Tree<T>, key: u64): TreePosition {
        let null = TreePosition {
            nodeID: 0,
            idx: 0,
        };
        let currNodeID = tree.root;
        while (currNodeID != 0) {
            let currNode = table::borrow(&tree.nodes, currNodeID);
            let isLeafNode = currNode.firstChild == 0;
            let size = vector::length(&currNode.elements);
            // Find insert idx.
            let insertIdx = 0;
            while (insertIdx < size) {
                let elem = vector::borrow(&currNode.elements, insertIdx);
                if (elem.key < key) {
                    insertIdx = insertIdx + 1;
                    continue
                };
                if (isLeafNode && elem.key == key) {
                    // We found the key.
                    return TreePosition {
                        nodeID: currNodeID,
                        idx: insertIdx,
                    }
                };
                break
            };
            // If this is a leaf node and we didn't find the key in the loop above, then the key doesn't exist in the
            // tree.
            if (isLeafNode) {
                break
            };
            // Otherwise, not a leaf node. Update the current node to be a child.
            let prevElemChild = if (insertIdx == 0) {
                currNode.firstChild
            } else {
                vector::borrow(&currNode.elements, insertIdx-1).child
            };
            currNodeID = prevElemChild;
        };
        null
    }

    // Assumes items are not already in the tree.
    // TODO: inline when bug is fixed (https://github.com/aptos-foundation/AIPs/issues/33#issuecomment-1399213932)
    fun tree_insert<T: copy + store + drop>(tree: &mut Tree<T>, key: u64, value: T) {
        let currNodeID = tree.root;
        let parentID = 0;
        let m = tree.m;
        tree.treeSize = tree.treeSize + 1;

        let elemItem =  TreeElem {
            key,
            value: vector[value],
            child: 0,
        };

        // Handle case when inserting into an empty tree.
        if (currNodeID == 0) {
            let rootNodeID = get_or_create_tree_node(tree);
            let rootNode = table::borrow_mut(&mut tree.nodes, rootNodeID);
            vector::push_back(&mut rootNode.elements, elemItem);
            tree.root = rootNodeID;
            tree.min = rootNodeID;
            tree.max = rootNodeID;
            return
        };

        loop {
            let currNode = table::borrow_mut(&mut tree.nodes, currNodeID);
            let isLeafNode = currNode.firstChild == 0;
            let size = vector::length(&currNode.elements);

            // Find insert idx.
            let insertIdx = 0;
            while (insertIdx < size) {
                let elem = vector::borrow(&currNode.elements, insertIdx);
                if (elem.key < key) {
                    insertIdx = insertIdx + 1;
                    continue
                };
                assert!(!isLeafNode || elem.key != key, ERR_TREE_DUPLICATE_ENTRY);
                break
            };
            let prevElemChild = if (insertIdx == 0) {
                currNode.firstChild
            } else {
                vector::borrow(&currNode.elements, insertIdx-1).child
            };
            // If this is a leaf node, add the element into it.
            if (isLeafNode) {
                // Inlined insert.
                vector::push_back(&mut currNode.elements, elemItem);
                let i = insertIdx;
                while (i < size) {
                    vector::swap(&mut currNode.elements, i, size);
                    i = i + 1;
                };
                size = size + 1;
            };
            // Resize nodes if needed - take the median and push it to the parent node. We can be sure the parent
            // node will have space for the median because it will have premeptively resized if needed in the last
            // iteration.
            let resizeTrigger = if (isLeafNode) {
                m+1
            } else {
                m
            };
            if (size >= resizeTrigger) {
                // Save some information for later.
                let currNodeNext = currNode.next;
                // Split node into two, about the median.
                let splitChildren = vector::empty();
                let medianIdx = size / 2;
                let iSplitChildren = medianIdx + 1;
                while (iSplitChildren < size) {
                    vector::push_back(&mut splitChildren, vector::pop_back(&mut currNode.elements));
                    iSplitChildren = iSplitChildren + 1;
                };
                // Inlined reverse.
                iSplitChildren = 0;
                let splitSize = vector::length(&splitChildren);
                let splitSizeHalf = splitSize / 2;
                while (iSplitChildren < splitSizeHalf) {
                    vector::swap(&mut splitChildren, iSplitChildren, splitSize - iSplitChildren - 1);
                    iSplitChildren = iSplitChildren + 1;
                };
                let medianElem = if (isLeafNode) {
                    // For leaf nodes, the element should be copied to parent nodes, not moved.
                    let elem = vector::borrow(&currNode.elements, medianIdx);
                    let cpy = TreeElem<T> {
                        key: elem.key,
                        value: vector[],
                        child: elem.child,
                    };
                    cpy
                } else {
                    // For internal nodes, the element should be moved to parent nodes.
                    vector::pop_back(&mut currNode.elements)
                };
                // Create new node.
                let newNodeID = get_or_create_tree_node(tree);
                let newNode = table::borrow_mut(&mut tree.nodes, newNodeID);
                // Insert node into the tree by setting pointers.
                newNode.firstChild = medianElem.child;
                medianElem.child = newNodeID;
                newNode.elements = splitChildren;
                // If the new node is a leaf node, connect it to all the leaf nodes.
                if (newNode.firstChild == 0) {
                    newNode.prev = currNodeID;
                    if (newNode.prev == 0) {
                        tree.min = newNodeID
                    };
                    newNode.next = currNodeNext;
                    if (newNode.next == 0) {
                        tree.max = newNodeID
                    };
                    // Need to re-borrow.
                    let currNode = table::borrow_mut(&mut tree.nodes, currNodeID);
                    currNode.next = newNodeID;
                    // Update previous pointer for next node.
                    if (currNodeNext != 0) {
                        let nextNode = table::borrow_mut(&mut tree.nodes, currNodeNext);
                        nextNode.prev = newNodeID;
                    };
                };
                // Add median to parent. Because of pre-emptive strategy, parent should have room.
                if (parentID == 0) {
                    let rootNodeID = get_or_create_tree_node(tree);
                    let rootNode = table::borrow_mut(&mut tree.nodes, rootNodeID);
                    rootNode.firstChild = currNodeID;
                    medianElem.child = newNodeID;
                    vector::push_back(&mut rootNode.elements, medianElem);
                    tree.root = rootNodeID;
                } else {
                    let parentNode = table::borrow_mut(&mut tree.nodes, parentID);
                    let parentSize = vector::length(&parentNode.elements);
                    let medianElemKey = medianElem.key;
                    // Inlined insert.
                    vector::push_back(&mut parentNode.elements, medianElem);
                    let idx = 0;
                    while (idx < parentSize) {
                        let elem = vector::borrow(&parentNode.elements, idx);
                        if (elem.key < medianElemKey) {
                            idx = idx + 1;
                            continue
                        };
                        vector::swap(&mut parentNode.elements, idx, parentSize);
                        idx = idx + 1;
                    };
                };
                // Update the current node and the insert index after splitting. If the index was larger than the
                // median, the element needs to be inserted into the new node. Otherwise, the element is inserted into
                // the the same node (which has now been resized to be smaller).
                if (insertIdx > medianIdx) {
                    currNodeID = newNodeID;
                };
            };
            if (isLeafNode) {
                // No more work is needed as the element has been added successfully.
                break
            };
            // Otherwise, not a leaf node. Update the current node to be a child.
            parentID = currNodeID;
            currNodeID = prevElemChild;
        };
    }

    // TODO: inline when bug is fixed (https://github.com/aptos-foundation/AIPs/issues/33#issuecomment-1399213932)
    fun tree_delete<T: copy + store + drop>(tree: &mut Tree<T>, key: u64) {
        // First find the item and record a path down to the key.
        let currNodeID = tree.root;
        let found = false;
        let path = vector<TreePathInfo<T>>[];
        let leftSibling = 0;
        let rightSibling = 0;
        while (currNodeID != 0) {
            let node = table::borrow(&tree.nodes, currNodeID);
            let size = vector::length(&node.elements);
            // If at a leaf node, find the element and terminate the outer loop.
            if (node.firstChild == 0) {
                let i = 0;
                let prevElemKey = 0;
                let prevElemValue = vector[];
                while (i < size) {
                    let elem = vector::borrow(&node.elements, i);
                    if (elem.key == key) {
                        found = true;
                        vector::push_back(&mut path, TreePathInfo {
                            nodeID: currNodeID,
                            rightElemIdx: i,
                            rightElemKey: elem.key,
                            rightElemValue: *&elem.value,
                            leftElemKey: prevElemKey,
                            leftElemValue: prevElemValue,
                            size,
                            leftSibling,
                            rightSibling,
                        });
                        break
                    };
                    prevElemKey = elem.key;
                    prevElemValue = elem.value;
                    i = i + 1;
                };
                break
            };
            // Otherwise, keep traversing down.
            let i = 0;
            let prevElem = TreeElem {
                key: 0,
                value: vector[],
                child: node.firstChild,
            };
            let prevPrevElemChild = 0;
            let found = false;
            while (i < size) {
                let elem = vector::borrow(&node.elements, i);
                if (key <= elem.key) {
                    vector::push_back(&mut path, TreePathInfo {
                        nodeID: currNodeID,
                        rightElemIdx: i,
                        rightElemKey: elem.key,
                        rightElemValue: vector[],
                        leftElemKey: prevElem.key,
                        leftElemValue: vector[],
                        size,
                        leftSibling,
                        rightSibling,
                    });
                    rightSibling = elem.child;
                    leftSibling = prevPrevElemChild;
                    currNodeID = prevElem.child;
                    found = true;
                    break
                };
                prevPrevElemChild = prevElem.child;
                prevElem = *elem;
                i = i + 1;
            };
            if (!found) {
                vector::push_back(&mut path, TreePathInfo {
                    nodeID: currNodeID,
                    rightElemIdx: i,
                    rightElemKey: 0,
                    rightElemValue: vector[],
                    leftElemKey: prevElem.key,
                    leftElemValue: vector[],
                    size,
                    leftSibling,
                    rightSibling,
                });
                rightSibling = 0;
                leftSibling = prevPrevElemChild;
                currNodeID = prevElem.child;
            }
        };

        assert!(found, ERR_TREE_ELEM_DOES_NOT_EXIST);

        // Once the item has been found, we need to start deleting.
        let m = tree.m;
        let i = 0;
        let pathSize = vector::length(&path);
        let itemPathInfo = vector::pop_back(&mut path);
        let deletedChildType = CHILD_TYPE_NULL;
        let treeRoot = tree.root;
        while(i < pathSize) {
            let unusedNodeStack = tree.unusedNodeStack;
            // First handle removal of element.
            let node = table::borrow_mut(&mut tree.nodes, itemPathInfo.nodeID);
            let isLeaf = node.firstChild == 0;
            if (isLeaf) {
                tree.treeSize = tree.treeSize - 1;
            };
            let isRoot = treeRoot == itemPathInfo.nodeID;
            //
            //  ([ ] [A] [B] [ ])
            //          |
            //  ([ ]) ([C]) ([ ])
            //
            //
            // We are currently looking at a PathInfo object describing A and B. If the node that was deleted in the
            // previous iteration was to the left of C, A should be deleted. Otherwise, the if the node that was
            // deleted was to the right of C, B should be deleted.
            //
            //       ( [B] [ ] )
            //        |
            //      ([C]) ([ ])
            //
            // The above situation can also happen, where B is the first element in it's node and C is its left child.
            // There will never be a node to the left of C in this case so we always delete B.
            //
            // If there was no deletion in the previous iteration, we can assume this is a leaf node and default to
            // deleting the rightElemIdx.
            let idxToDelete = if (deletedChildType == CHILD_TYPE_LEFT && itemPathInfo.rightElemIdx > 0) {
                itemPathInfo.rightElemIdx - 1
            } else {
                itemPathInfo.rightElemIdx
            };
            let deletedElem = vector::remove(&mut node.elements, idxToDelete);
            // If we deleted a child of the current node in the previous iteration, update the pointers to the children.
            // Should only a require an update if the left of the removed element was deleted.
            if (deletedChildType == CHILD_TYPE_LEFT) {
                if (idxToDelete == 0) {
                    //
                    //       ( [B] [ ] )
                    //        |   |
                    //     ([C]) ([D])
                    //
                    // If [B] and ([C]) were deleted. New first child of node should be right child of B, ([D]).
                    node.firstChild = deletedElem.child;
                } else {
                    //
                    //       ( [A] [B] [ ] )
                    //            |   |
                    //         ([C]) ([D])
                    //
                    // If [B] and ([C]) were deleted. New right child of A should be right child of B, ([D]).
                    //
                    // Even though we removed an element, the previous element should still have the same index.
                    let prevElem = vector::borrow_mut(&mut node.elements, idxToDelete - 1);
                    prevElem.child = deletedElem.child;
                };
            };
            itemPathInfo.size = itemPathInfo.size - 1;

            // If node still has enough elems, can return.
            let minSize = if (isRoot) {
                0
            } else if (isLeaf) {
                m/2
            } else {
                m/2 - 1
            };
            if (itemPathInfo.size >= minSize) {
                // If this is the root and we just deleted the last element, update the tree root property.
                if (isRoot && itemPathInfo.size == 0) {
                    let firstChild = node.firstChild;
                    node.firstChild = 0;
                    node.next = unusedNodeStack;
                    tree.unusedNodeStack = itemPathInfo.nodeID;
                    node.prev = 0;
                    if (tree.min == tree.root) {
                        tree.min = 0;
                    };
                    if (tree.max == tree.root) {
                        tree.max = 0;
                    };
                    // firstChild will always be set to the one child the root has because we update it when deleting the
                    // element from the root.
                    tree.root = firstChild;
                };
                return
            };
            // Check if the next node has an elem to spare.
            // If it does, we adopt the element.
            //
            //       ([A] [F] [K] [Z])
            //           |   |   |
            //           |   |   ([M] [N])
            // ([B] [C])     |
            //               ([G] [H] [I])
            //
            // Adopt G from next node.
            //
            //       ([A] [F] [K] [Z])
            //           |   |   |
            //           |   |   ([M] [N])
            // ([B] [C] [F]) |
            //               ([H] [I])
            //
            //
            let nextNodeSmall = false;
            if (itemPathInfo.rightSibling != 0) {
                let nextNode = table::borrow_mut(&mut tree.nodes, itemPathInfo.rightSibling);
                let nextNodeSize = vector::length(&nextNode.elements);
                nextNodeSmall = nextNodeSize <= minSize;
                if (!nextNodeSmall) {
                    // We can adopt from the next node because it has enough elements.
                    // Remove first element.
                    let i = 1;
                    while (i < nextNodeSize) {
                        vector::swap(&mut nextNode.elements, i, i-1);
                        i = i + 1;
                    };
                    let elem = vector::pop_back(&mut nextNode.elements);
                    // Update children info.
                    let firstChild = nextNode.firstChild;
                    nextNode.firstChild = elem.child;
                    elem.child = firstChild;
                    // Update the parent node.
                    let parentPathInfo = vector::pop_back(&mut path);
                    let parentNode = table::borrow_mut(&mut tree.nodes, parentPathInfo.nodeID);
                    // For there to be a right sibling, rightElemIdx must be < length(parentNode.elements).
                    let parentElem = vector::borrow_mut(&mut parentNode.elements, parentPathInfo.rightElemIdx);
                    let parentElemKey = parentElem.key;
                    parentElem.key = elem.key;
                    if (!isLeaf) {
                        // For internal nodes, the element we are borrowing from the right sibling will have children.
                        // For satisfy element bound conditions, the element value inserted into the current node can't
                        // be the same value as the element we are borrowing because the element's left child will
                        // have no where to go. To account for this case, we set the value of the element to be the old
                        // value of the parent element.
                        //
                        // There is no need to worry about the case where the parent element == the node element because
                        // that can only happen for leaf nodes.
                        elem.key = parentElemKey;
                    };
                    // Push adopted element to back.
                    let node = table::borrow_mut(&mut tree.nodes, itemPathInfo.nodeID);
                    vector::push_back(&mut node.elements, elem);
                    return
                }
            };
            // Else, check if the prev node has a elem to spare.
            //
            //       ([A] [F] [K] [Z])
            //           |   |   |
            //           |   |   ([M] [N])
            // ([B] [C] [D]) |
            //               ([H] [I])
            //
            // Adopt D from prev node.
            //
            //       ([A] [C] [K] [Z])
            //           |   |   |
            //           |   |   ([M] [N])
            //   ([B] [C])   |
            //               ([D] [H] [I])
            //
            let prevNodeSmall = false;
            if (itemPathInfo.leftSibling != 0) {
                let prevNode = table::borrow_mut(&mut tree.nodes, itemPathInfo.leftSibling);
                let prevNodeSize = vector::length(&prevNode.elements);
                prevNodeSmall = prevNodeSize <= minSize;
                if (!prevNodeSmall) {
                    // We can adopt from the prev node because it has enough elements.
                    // Remove last element from node on the left.
                    let elem = vector::pop_back(&mut prevNode.elements);
                    // Prev node should always have > 1 elems because it should always have > m/2 - 1 elems in this
                    // block (!prevNodeSmall) and min value for m is 4. Therefore, the node will always
                    // have > 4/2 - 1 = 1 elems.
                    let prevElemKey = vector::borrow(&prevNode.elements, prevNodeSize - 2).key;
                    // Update the parent node.
                    let parentPathInfo = vector::pop_back(&mut path);
                    let parentNode = table::borrow_mut(&mut tree.nodes, parentPathInfo.nodeID);
                    // Since we are adopting from the prev node, there is a left sibling which means
                    // rightElemIdx must be > 0.
                    let parentElem = vector::borrow_mut(&mut parentNode.elements, parentPathInfo.rightElemIdx - 1);
                    // Note that we need to distinguish between leaf nodes and internal nodes for left adoptions
                    // because the node being adopted to can have an element equal to its parent when the node is a
                    // leaf:
                    //         ([10]   [20])
                    //         |     |     |
                    //  ([5] [10])   |    ([21] [22])
                    //               |
                    //          ([11] [12])
                    //
                    // In the above example, if 10 was being adopted into ([11 12]), the new tree should look like:
                    //
                    //         ([5]   [20])
                    //         |    |     |
                    //      ([5])   |    ([21] [22])
                    //              |
                    //       ([10] [11] [12])
                    //
                    // However, if the nodes being adopted from were not leafs:
                    //
                    //         ([10]   [20])
                    //         |     |     |
                    //  ([5] [7])    |     ([21] [22])
                    //   |  |   |    |
                    //   A  B   C   ([11] [12])
                    //               |    |    |
                    //               D    E    F
                    //
                    // Then the tree post adoption would look like:
                    //
                    //         ([7]    [20])
                    //         |    |   |
                    //     ([5])    |   ([21] [22])
                    //     |  |     |
                    //     A  B    ([10] [11] [12])
                    //              |   |    |    |
                    //              C   D    E    F
                    //
                    // Note how C was only able to be placed where it is because [10] was copied down from the parent.
                    if (isLeaf) {
                        // For leaf nodes, since none of the elements have children, we can just adopt and then update
                        // the parent element to be the value of the new last element (prevElem) of the left node.
                        parentElem.key = prevElemKey;
                    } else {
                        // For internal nodes, the element we are borrowing from the left sibling will have children.
                        // For satisfy element bound conditions, the element value inserted into the current node can't
                        // be the same value as the element we are borrowing because the element's right child will
                        // have no where to go. To account for this case, we set the value of the element to be the
                        // old value of theparent element.
                        //
                        // There is no need to worry about the case where the parent element == the node element because
                        // that can only happen for leaf nodes.
                        let parentElemValue = parentElem.key;
                        parentElem.key = elem.key;
                        elem.key = parentElemValue;
                    };
                    // Push adopted element to front.
                    let node = table::borrow_mut(&mut tree.nodes, itemPathInfo.nodeID);
                    let elemChild = elem.child;
                    elem.child = node.firstChild;
                    node.firstChild = elemChild;
                    vector::push_back(&mut node.elements, elem);
                    let i = 0;
                    while (i < itemPathInfo.size) {
                        vector::swap(&mut node.elements, i, itemPathInfo.size);
                        i = i + 1;
                    };
                    return
                }
            };
            // Neither immediate siblings had enough to give. we'll merge and then delete an element from the parent.
            let nodeToMergeID = if (nextNodeSmall) {
                deletedChildType = CHILD_TYPE_RIGHT;
                itemPathInfo.rightSibling
            } else if (prevNodeSmall) {
                deletedChildType = CHILD_TYPE_LEFT;
                itemPathInfo.leftSibling
            } else {
                abort 0
            };
            // Save info about the node and add it to the unsed node stack.
            let unusedNodeStack = tree.unusedNodeStack;
            let nodetoMerge = table::borrow_mut(&mut tree.nodes, nodeToMergeID);
            let nodeToMergeElements = nodetoMerge.elements;
            let nodeToMergeFirstChild = nodetoMerge.firstChild;
            let nodeToMergeNext = nodetoMerge.next;
            let nodeToMergePrev = nodetoMerge.prev;
            nodetoMerge.elements = vector::empty();
            nodetoMerge.firstChild = 0;
            nodetoMerge.prev = 0;
            nodetoMerge.next = unusedNodeStack;
            tree.unusedNodeStack = nodeToMergeID;
            if (nodeToMergeID == tree.max) {
                tree.max = nodeToMergePrev;
            };
            if (nodeToMergeID == tree.min) {
                tree.min = nodeToMergeNext;
            };
            // Push the elements from the deleted node into the node that lives.
            let parentPathInfo = vector::pop_back(&mut path);
            let node = table::borrow_mut(&mut tree.nodes, itemPathInfo.nodeID);
            if (deletedChildType == CHILD_TYPE_RIGHT) {
                //
                //       ( [A]   [B] )
                //        |    |    |
                //     ([E]) ([C]) ([D])
                //
                // We're merging ([C]) and ([D]) and ([D]) gets deleted.
                // We only need to include B if nodes containing C and D are not leafs.
                if (!isLeaf) {
                    vector::push_back(&mut node.elements, TreeElem {
                        key: parentPathInfo.rightElemKey,
                        value: parentPathInfo.rightElemValue,
                        child: nodeToMergeFirstChild,
                    });
                };
                // Inlined append.
                let i = 0;
                let size = vector::length(&nodeToMergeElements);
                let halfSize = size/2;
                while (i < halfSize) { // Reverse.
                    vector::swap(&mut nodeToMergeElements, i, size - i - 1);
                    i = i + 1;
                };
                i = 0;
                while (i < size) { // Pop from vector to append into destination vector.
                    vector::push_back(&mut node.elements, vector::pop_back(&mut nodeToMergeElements));
                    i = i + 1;
                };
            } else {
                //
                //       ( [A]   [B] )
                //        |    |    |
                //     ([E]) ([C]) ([D])
                //
                // We're merging ([C]) and ([E]) and ([E]) gets deleted.
                // We only need to include B if nodes containing C and E are not leafs.
                if (!isLeaf) {
                    vector::push_back(&mut nodeToMergeElements, TreeElem {
                        key: parentPathInfo.leftElemKey,
                        value: parentPathInfo.leftElemValue,
                        child: node.firstChild,
                    });
                };
                // Inlined append.
                let i = 0;
                let halfSize = itemPathInfo.size/2;
                while (i < halfSize) { // Reverse.
                    vector::swap(&mut node.elements, i, itemPathInfo.size - i - 1);
                    i = i + 1;
                };
                i = 0;
                while (i < itemPathInfo.size) { // Pop from vector to append into destination vector.
                    vector::push_back(&mut nodeToMergeElements, vector::pop_back(&mut node.elements));
                    i = i + 1;
                };
                node.elements = nodeToMergeElements;
                node.firstChild = nodeToMergeFirstChild;
            };
            // Update node next and prev, if they exist.
            if (node.next == nodeToMergeID) {
                node.next = nodeToMergeNext;
                if (nodeToMergeNext != 0) {
                    let next = table::borrow_mut(&mut tree.nodes, nodeToMergeNext);
                    next.prev = itemPathInfo.nodeID;
                };
            } else if (node.prev == nodeToMergeID) {
                node.prev = nodeToMergePrev;
                if (nodeToMergePrev != 0) {
                    let prev = table::borrow_mut(&mut tree.nodes, nodeToMergePrev);
                    prev.next = itemPathInfo.nodeID;
                };
            };

            // Kill the parent.
            itemPathInfo = parentPathInfo;
            i = i + 1;
        };
    }

    inline fun tree_iterate<T: copy + store + drop>(tree: &Tree<T>, type: u8): TreeIterator {
        if (tree.treeSize == 0) {
            TreeIterator {
                pos: TreePosition {
                    nodeID: 0,
                    idx: tree.m+1,
                },
                type,
            }
        } else {
            let (nodeID, idx) = if (type == INCREASING_ITERATOR) {
                (tree.min, 0)
            } else if (type == DECREASING_ITERATOR) {
                let node = table::borrow(&tree.nodes, tree.max);
                (tree.max, vector::length(&node.elements) - 1)
            } else {
                abort ERR_TREE_INVALID_ITERATOR_TYPE
            };
            TreeIterator {
                pos: TreePosition {
                    nodeID,
                    idx,
                },
                type,
            }
        }
    }

    inline fun tree_get_next_mut<T: copy + store + drop>(tree: &mut Tree<T>, it: &mut TreeIterator): (u64, &mut T) {
        let pos = it.pos;
        tree_next(tree, it);
        tree_get_mut(tree, &pos)
    }

    inline fun tree_get_next<T: copy + store + drop>(tree: &Tree<T>, it: &mut TreeIterator): (u64, &T) {
        let pos = it.pos;
        tree_next(tree, it);
        tree_get(tree, &pos)
    }

    inline fun tree_get_mut<T: copy + store + drop>(tree: &mut Tree<T>, pos: &TreePosition): (u64, &mut T) {
        let node = table::borrow_mut(&mut tree.nodes, pos.nodeID);
        let elem = vector::borrow_mut(&mut node.elements, pos.idx);
        assert!(vector::length(&elem.value) > 0, ERR_TREE_ELEM_DOES_NOT_EXIST);
        (elem.key, vector::borrow_mut(&mut elem.value, 0))
    }

    inline fun tree_get<T: copy + store + drop>(tree: &Tree<T>, pos: &TreePosition): (u64, &T) {
        let node = table::borrow(&tree.nodes, pos.nodeID);
        let elem = vector::borrow(&node.elements, pos.idx);
        assert!(vector::length(&elem.value) > 0, ERR_TREE_ELEM_DOES_NOT_EXIST);
        (elem.key, vector::borrow(&elem.value, 0))
    }

    inline fun tree_pop_max<T: copy + store + drop>(tree: &mut Tree<T>): (u64, T) {
        let node = table::borrow(&tree.nodes, tree.max);
        let elem = vector::borrow(&node.elements, vector::length(&node.elements) - 1);
        assert!(vector::length(&elem.value) > 0, ERR_TREE_ELEM_DOES_NOT_EXIST);
        let price = elem.key;
        let elem = *vector::borrow(&elem.value, 0);
        tree_delete(tree, price);
        (price, elem)
    }

    inline fun tree_pop_min<T: copy + store + drop>(tree: &mut Tree<T>): (u64, T) {
        let node = table::borrow(&tree.nodes, tree.min);
        let elem = vector::borrow(&node.elements, 0);
        assert!(vector::length(&elem.value) > 0, ERR_TREE_ELEM_DOES_NOT_EXIST);
        let price = elem.key;
        let elem = *vector::borrow(&elem.value, 0);
        tree_delete(tree, price);
        (price, elem)
    }

    inline fun tree_next<T: copy + store + drop>(tree: &Tree<T>, it: &mut TreeIterator) {
        assert!(it.pos.nodeID != 0, ERR_TREE_EMPTY_ITERATOR);
        let node = table::borrow(&tree.nodes, it.pos.nodeID);
        if (it.type == DECREASING_ITERATOR) {
            if (it.pos.idx > 0) {
                it.pos.idx = it.pos.idx - 1;
            } else {
                it.pos.nodeID = node.prev;
                if (it.pos.nodeID > 0) {
                    let prevNode = table::borrow(&tree.nodes, it.pos.nodeID);
                    it.pos.idx = vector::length(&prevNode.elements) - 1;
                }
            };
        } else {
            if (it.pos.idx < vector::length(&node.elements) - 1) {
                it.pos.idx = it.pos.idx + 1;
            } else {
                it.pos.nodeID = node.next;
                it.pos.idx = 0;
            };
        };
    }

    inline fun get_or_create_tree_node<T: copy + store + drop>(tree: &mut Tree<T>): u16 {
        if (tree.unusedNodeStack == 0) {
            prealloc_tree_nodes(tree, 1);
        };
        let nodeID = tree.unusedNodeStack;
        let node = table::borrow_mut(&mut tree.nodes, nodeID);
        tree.unusedNodeStack = node.next;
        node.next = 0;
        nodeID
    }

    // <editor-fold defaultstate="collapsed" desc="B-Tree Tests">

    // Fuzz Tests.

    // // <editor-fold defaultstate="collapsed" desc="B-Tree Fuzz Tests">
    //
    // #[test]
    // fun fuzz_test_tree_mass_inserts() {
    //     // Tests that a tree is created and is valid with [0, 1000] elements inserted in increasing, decreasing, and
    //     // random order.
    //
    //     let (tree, _) = gen_tree_sequential_random_order(8, 1000);
    //     assert_valid_tree(&tree);
    //     assert_contains_range(&tree, 1, 1000);
    //     destroy_tree(tree);
    //
    //     let (tree, _) = gen_tree_sequential_increasing_order(8, 1000);
    //     assert_valid_tree(&tree);
    //     assert_contains_range(&tree, 1, 1000);
    //     destroy_tree(tree);
    //
    //     let (tree, _) = gen_tree_sequential_decreasing_order(8, 1000);
    //     assert_valid_tree(&tree);
    //     assert_contains_range(&tree, 1, 1000);
    //     destroy_tree(tree);
    // }
    //
    // #[test]
    // fun fuzz_test_tree_random_deletes_random_inserts() {
    //     // Tests that a elements from tree can be randomly deleted from a tree that is created from random inserts.
    //     // Note that requires max gas on aptos cli to be set.
    //
    //     let (tree, elems) = gen_tree_sequential_random_order(8, 1000);
    //     assert_valid_tree(&tree);
    //     while (!vector::is_empty(&elems)) {
    //         let l = vector::length(&elems);
    //         let l3 = l * l * l;
    //         let i = 18446744073709551615 % l3 % l;
    //         let elem = vector::swap_remove(&mut elems, i);
    //         tree_delete(&mut tree, elem);
    //         assert_valid_tree(&tree);
    //     };
    //     assert!(tree.treeSize == 0, 0);
    //     destroy_tree(tree);
    // }
    //
    // #[test]
    // fun fuzz_test_tree_random_deletes_increasing_inserts() {
    //     // Tests that a elements from tree can be randomly deleted from a tree that is created from increasing inserts.
    //     // Note that requires max gas on aptos cli to be set.
    //
    //     let (tree, elems) = gen_tree_sequential_increasing_order(8, 1000);
    //     assert_valid_tree(&tree);
    //     while (!vector::is_empty(&elems)) {
    //         let l = vector::length(&elems);
    //         let l3 = l * l * l;
    //         let i = 18446744073709551615 % l3 % l;
    //         let elem = vector::swap_remove(&mut elems, i);
    //         tree_delete(&mut tree, elem);
    //         assert_valid_tree(&tree);
    //     };
    //     assert!(tree.treeSize == 0, 0);
    //     destroy_tree(tree);
    // }
    //
    // #[test]
    // fun fuzz_test_tree_random_deletes_decreasing_inserts() {
    //     // Tests that a elements from tree can be randomly deleted from a tree that is created from decreasing inserts.
    //     // Note that requires max gas on aptos cli to be set.
    //
    //     let (tree, elems) = gen_tree_sequential_decreasing_order(8, 1000);
    //     assert_valid_tree(&tree);
    //     while (!vector::is_empty(&elems)) {
    //         let l = vector::length(&elems);
    //         let l3 = l * l * l;
    //         let i = 18446744073709551615 % l3 % l;
    //         let elem = vector::swap_remove(&mut elems, i);
    //         tree_delete(&mut tree, elem);
    //         assert_valid_tree(&tree);
    //     };
    //     assert!(tree.treeSize == 0, 0);
    //     destroy_tree(tree);
    // }
    //
    // #[test]
    // fun fuzz_test_tree_increasing_deletes_random_inserts() {
    //     // Tests that a elements from tree can be deleted in increasing order from a tree that is created from
    //     // random inserts.
    //     // Note that requires max gas on aptos cli to be set.
    //
    //     let (tree, elems) = gen_tree_sequential_random_order(8, 1000);
    //     assert_valid_tree(&tree);
    //     let i = 0;
    //     let size = vector::length(&elems);
    //     while (i < size) {
    //         tree_delete(&mut tree, i + 1);
    //         assert_valid_tree(&tree);
    //         i = i + 1;
    //     };
    //     assert!(tree.treeSize == 0, 0);
    //     destroy_tree(tree);
    // }
    //
    // #[test]
    // fun fuzz_test_tree_increasing_deletes_increasing_inserts() {
    //     // Tests that a elements from tree can be deleted in increasing order from a tree that is created from
    //     // inreasing inserts.
    //     // Note that requires max gas on aptos cli to be set.
    //
    //     let (tree, elems) = gen_tree_sequential_increasing_order(8, 1000);
    //     assert_valid_tree(&tree);
    //     let i = 0;
    //     let size = vector::length(&elems);
    //     while (i < size) {
    //         tree_delete(&mut tree, i + 1);
    //         assert_valid_tree(&tree);
    //         i = i + 1;
    //     };
    //     assert!(tree.treeSize == 0, 0);
    //     destroy_tree(tree);
    // }
    //
    // #[test]
    // fun fuzz_test_tree_increasing_deletes_decreasing_inserts() {
    //     // Tests that a elements from tree can be deleted in increasing order from a tree that is created from
    //     // decreasing inserts.
    //     // Note that requires max gas on aptos cli to be set.
    //
    //     let (tree, elems) = gen_tree_sequential_decreasing_order(8, 1000);
    //     assert_valid_tree(&tree);
    //     let i = 0;
    //     let size = vector::length(&elems);
    //     while (i < size) {
    //         tree_delete(&mut tree, i + 1);
    //         assert_valid_tree(&tree);
    //         i = i + 1;
    //     };
    //     assert!(tree.treeSize == 0, 0);
    //     destroy_tree(tree);
    // }
    //
    // #[test]
    // fun fuzz_test_tree_decreasing_deletes_random_inserts() {
    //     // Tests that a elements from tree can be deleted in decreasing order from a tree that is created from
    //     // random inserts.
    //     // Note that requires max gas on aptos cli to be set.
    //
    //     let (tree, elems) = gen_tree_sequential_random_order(8, 1000);
    //     assert_valid_tree(&tree);
    //     let i = 0;
    //     let size = vector::length(&elems);
    //     while (i < size) {
    //         tree_delete(&mut tree, size - i);
    //         assert_valid_tree(&tree);
    //         i = i + 1;
    //     };
    //     assert!(tree.treeSize == 0, 0);
    //     destroy_tree(tree);
    // }
    //
    // #[test]
    // fun fuzz_test_tree_decreasing_deletes_increasing_inserts() {
    //     // Tests that a elements from tree can be deleted in decreasing order from a tree that is created from
    //     // inreasing inserts.
    //     // Note that requires max gas on aptos cli to be set.
    //
    //     let (tree, elems) = gen_tree_sequential_increasing_order(8, 1000);
    //     assert_valid_tree(&tree);
    //     let i = 0;
    //     let size = vector::length(&elems);
    //     while (i < size) {
    //         tree_delete(&mut tree, size - i);
    //         assert_valid_tree(&tree);
    //         i = i + 1;
    //     };
    //     assert!(tree.treeSize == 0, 0);
    //     destroy_tree(tree);
    // }
    //
    // #[test]
    // fun fuzz_test_tree_decreasing_deletes_decreasing_inserts() {
    //     // Tests that a elements from tree can be deleted in decreasing order from a tree that is created from
    //     // decreasing inserts.
    //     // Note that requires max gas on aptos cli to be set.
    //
    //     let (tree, elems) = gen_tree_sequential_decreasing_order(8, 1000);
    //     assert_valid_tree(&tree);
    //     let i = 0;
    //     let size = vector::length(&elems);
    //     while (i < size) {
    //         tree_delete(&mut tree, size - i);
    //         assert_valid_tree(&tree);
    //         i = i + 1;
    //     };
    //     assert!(tree.treeSize == 0, 0);
    //     destroy_tree(tree);
    // }
    //
    // #[test]
    // fun fuzz_test_tree_random_inserts_and_deletes() {
    //     // Fuzz tests inserts and deletes.
    //
    //     let seed = 18446744073709551615u64;
    //
    //     let allElems = ftu::gen_random_list(10000, 1, 1000000);
    //     let elemsToAdd = vector[];
    //     let i = 0;
    //     while (i < 1000) {
    //         vector::push_back(&mut elemsToAdd, vector::pop_back(&mut allElems));
    //         i = i + 1;
    //     };
    //     let treeElems = vector[];
    //     let treeValues = vector[];
    //     while (i < 5000) {
    //         let elem = vector::pop_back(&mut allElems);
    //         vector::push_back(&mut treeElems, elem);
    //         vector::push_back(&mut treeValues, ((elem * 2 % MAX_U16) as u16));
    //         i = i + 1;
    //     };
    //     let tree = build_tree(6, treeElems, treeValues);
    //     assert_valid_tree(&tree);
    //
    //     let i = 0;
    //     while (i < 100) {
    //         let i3 = (i+1) * (i+1) * (i+1);
    //         // Decide if to insert or delete.
    //         if (seed % i3 % 2 == 0) {
    //             // Delete.
    //             let deleteIdx = seed % i3 % vector::length(&treeElems);
    //             let elem = vector::swap_remove(&mut treeElems, deleteIdx);
    //             tree_delete(&mut tree, elem);
    //             assert_valid_tree(&tree);
    //         } else {
    //             // Insert.
    //             let insertElemIdx = seed % i3 % vector::length(&elemsToAdd);
    //             let elem = vector::swap_remove(&mut elemsToAdd, insertElemIdx);
    //             let value = ((elem * 2 % MAX_U16) as u16);
    //             tree_insert(&mut tree, elem, value);
    //             assert_valid_tree(&tree);
    //             vector::push_back(&mut treeElems, elem);
    //         };
    //         i = i + 1;
    //     };
    //
    //     destroy_tree(tree);
    // }
    //
    // // </editor-fold>

    // Specific Tests.

    #[test]
    fun test_tree_pop_max() {
        let tree = tree_from_strs(4, vector[
            s(b"(3 Keys:[ 50 70 100 ] Children:[ 4 5 6 7 ] Leaf:f Prev:0 Next:0)"),
            s(b"(4 Keys:[ 10 40 ] Children:[ 8 9 10 ] Leaf:f Prev:0 Next:0)"),
            s(b"(5 Keys:[ 56 65 ] Children:[ 11 12 13 ] Leaf:f Prev:0 Next:0)"),
            s(b"(6 Keys:[ 85 95 ] Children:[ 14 15 16 ] Leaf:f Prev:0 Next:0)"),
            s(b"(7 Keys:[ 105 120 ] Children:[ 17 18 19 ] Leaf:f Prev:0 Next:0)"),

            s(b"(8 Keys:[ 5 6 7 ] Values:[ 7 6 5 ] Children:[ 0 0 0 0 ] Leaf:t Prev:0 Next:9)"),
            s(b"(9 Keys:[ 11 12 13 ] Values:[ 13 12 11 ] Children:[ 0 0 0 0 ] Leaf:t Prev:8 Next:10)"),
            s(b"(10 Keys:[ 41 42 43 ] Values:[ 43 42 41 ] Children:[ 0 0 0 0 ] Leaf:t Prev:9 Next:11)"),
            s(b"(11 Keys:[ 54 55 56 ] Values:[ 56 55 54 ] Children:[ 0 0 0 0 ] Leaf:t Prev:10 Next:12)"),
            s(b"(12 Keys:[ 57 58 59 ] Values:[ 59 58 57 ] Children:[ 0 0 0 0 ] Leaf:t Prev:11 Next:13)"),
            s(b"(13 Keys:[ 66 67 68 ] Values:[ 68 67 66 ] Children:[ 0 0 0 0 ] Leaf:t Prev:12 Next:14)"),
            s(b"(14 Keys:[ 76 77 78 ] Values:[ 78 77 76 ] Children:[ 0 0 0 0 ] Leaf:t Prev:13 Next:15)"),
            s(b"(15 Keys:[ 86 87 88 ] Values:[ 88 87 86 ] Children:[ 0 0 0 0 ] Leaf:t Prev:14 Next:16)"),
            s(b"(16 Keys:[ 96 97 98 ] Values:[ 98 97 96 ] Children:[ 0 0 0 0 ] Leaf:t Prev:15 Next:17)"),
            s(b"(17 Keys:[ 101 102 103 ] Values:[ 103 102 101 ] Children:[ 0 0 0 0 ] Leaf:t Prev:16 Next:18)"),
            s(b"(18 Keys:[ 109 110 111 ] Values:[ 111 110 109 ] Children:[ 0 0 0 0 ] Leaf:t Prev:17 Next:19)"),
            s(b"(19 Keys:[ 121 122 123 ] Values:[ 123 122 121 ] Children:[ 0 0 0 0 ] Leaf:t Prev:18 Next:0)"),
        ]);
        assert_valid_tree(&tree);
        let (price, elem) = tree_pop_max(&mut tree);
        assert!(price == 123, 0);
        assert!(elem == 121, 0);
        let (price, elem) = tree_pop_max(&mut tree);
        assert!(price == 122, 0);
        assert!(elem == 122, 0);
        let (price, elem) = tree_pop_max(&mut tree);
        assert!(price == 121, 0);
        assert!(elem == 123, 0);
        let (price, elem) = tree_pop_max(&mut tree);
        assert!(price == 111, 0);
        assert!(elem == 109, 0);
        assert_valid_tree(&tree);

        destroy_tree(tree);
    }

    #[test]
    fun test_tree_pop_max_to_empty_tree() {
        let tree = tree_from_strs(4, vector[
            s(b"(8 Keys:[ 5 ] Values:[ 7 ] Children:[ 0 0 ] Leaf:t Prev:0 Next:0)"),
        ]);
        assert_valid_tree(&tree);
        let (price, elem) = tree_pop_max(&mut tree);
        assert!(price == 5, 0);
        assert!(elem == 7, 0);
        assert_tree(&tree, vector[]);
        assert!(tree.treeSize == 0, 0);
        assert_valid_tree(&tree);
        destroy_tree(tree);
    }

    #[test]
    fun test_tree_pop_min() {
        let tree = tree_from_strs(4, vector[
            s(b"(3 Keys:[ 50 70 100 ] Children:[ 4 5 6 7 ] Leaf:f Prev:0 Next:0)"),
            s(b"(4 Keys:[ 10 40 ] Children:[ 8 9 10 ] Leaf:f Prev:0 Next:0)"),
            s(b"(5 Keys:[ 56 65 ] Children:[ 11 12 13 ] Leaf:f Prev:0 Next:0)"),
            s(b"(6 Keys:[ 85 95 ] Children:[ 14 15 16 ] Leaf:f Prev:0 Next:0)"),
            s(b"(7 Keys:[ 105 120 ] Children:[ 17 18 19 ] Leaf:f Prev:0 Next:0)"),

            s(b"(8 Keys:[ 5 6 7 ] Values:[ 7 6 5 ] Children:[ 0 0 0 0 ] Leaf:t Prev:0 Next:9)"),
            s(b"(9 Keys:[ 11 12 13 ] Values:[ 13 12 11 ] Children:[ 0 0 0 0 ] Leaf:t Prev:8 Next:10)"),
            s(b"(10 Keys:[ 41 42 43 ] Values:[ 43 42 41 ] Children:[ 0 0 0 0 ] Leaf:t Prev:9 Next:11)"),
            s(b"(11 Keys:[ 54 55 56 ] Values:[ 56 55 54 ] Children:[ 0 0 0 0 ] Leaf:t Prev:10 Next:12)"),
            s(b"(12 Keys:[ 57 58 59 ] Values:[ 59 58 57 ] Children:[ 0 0 0 0 ] Leaf:t Prev:11 Next:13)"),
            s(b"(13 Keys:[ 66 67 68 ] Values:[ 68 67 66 ] Children:[ 0 0 0 0 ] Leaf:t Prev:12 Next:14)"),
            s(b"(14 Keys:[ 76 77 78 ] Values:[ 78 77 76 ] Children:[ 0 0 0 0 ] Leaf:t Prev:13 Next:15)"),
            s(b"(15 Keys:[ 86 87 88 ] Values:[ 88 87 86 ] Children:[ 0 0 0 0 ] Leaf:t Prev:14 Next:16)"),
            s(b"(16 Keys:[ 96 97 98 ] Values:[ 98 97 96 ] Children:[ 0 0 0 0 ] Leaf:t Prev:15 Next:17)"),
            s(b"(17 Keys:[ 101 102 103 ] Values:[ 103 102 101 ] Children:[ 0 0 0 0 ] Leaf:t Prev:16 Next:18)"),
            s(b"(18 Keys:[ 109 110 111 ] Values:[ 111 110 109 ] Children:[ 0 0 0 0 ] Leaf:t Prev:17 Next:19)"),
            s(b"(19 Keys:[ 121 122 123 ] Values:[ 123 122 121 ] Children:[ 0 0 0 0 ] Leaf:t Prev:18 Next:0)"),
        ]);
        assert_valid_tree(&tree);
        let (price, elem) = tree_pop_min(&mut tree);
        assert!(price == 5, 0);
        assert!(elem == 7, 0);
        let (price, elem) = tree_pop_min(&mut tree);
        assert!(price == 6, 0);
        assert!(elem == 6, 0);
        let (price, elem) = tree_pop_min(&mut tree);
        assert!(price == 7, 0);
        assert!(elem == 5, 0);
        let (price, elem) = tree_pop_min(&mut tree);
        assert!(price == 11, 0);
        assert!(elem == 13, 0);
        assert_valid_tree(&tree);

        destroy_tree(tree);
    }

    #[test]
    fun test_tree_pop_min_to_empty_tree() {
        let tree = tree_from_strs(4, vector[
            s(b"(8 Keys:[ 5 ] Values:[ 7 ] Children:[ 0 0 ] Leaf:t Prev:0 Next:0)"),
        ]);
        assert_valid_tree(&tree);
        let (price, elem) = tree_pop_min(&mut tree);
        assert!(price == 5, 0);
        assert!(elem == 7, 0);
        assert_tree(&tree, vector[]);
        assert!(tree.treeSize == 0, 0);
        assert_valid_tree(&tree);
        destroy_tree(tree);
    }

    #[test]
    fun test_tree_iterate_increasing() {
        // Tests that iteration works in the increasing direction.

        let tree = tree_from_strs(4, vector[
            s(b"(3 Keys:[ 50 70 100 ] Children:[ 4 5 6 7 ] Leaf:f Prev:0 Next:0)"),
            s(b"(4 Keys:[ 10 40 ] Children:[ 8 9 10 ] Leaf:f Prev:0 Next:0)"),
            s(b"(5 Keys:[ 56 65 ] Children:[ 11 12 13 ] Leaf:f Prev:0 Next:0)"),
            s(b"(6 Keys:[ 85 95 ] Children:[ 14 15 16 ] Leaf:f Prev:0 Next:0)"),
            s(b"(7 Keys:[ 105 120 ] Children:[ 17 18 19 ] Leaf:f Prev:0 Next:0)"),

            s(b"(8 Keys:[ 5 6 7 ] Values:[ 7 6 5 ] Children:[ 0 0 0 0 ] Leaf:t Prev:0 Next:9)"),
            s(b"(9 Keys:[ 11 12 13 ] Values:[ 13 12 11 ] Children:[ 0 0 0 0 ] Leaf:t Prev:8 Next:10)"),
            s(b"(10 Keys:[ 41 42 43 ] Values:[ 43 42 41 ] Children:[ 0 0 0 0 ] Leaf:t Prev:9 Next:11)"),
            s(b"(11 Keys:[ 54 55 56 ] Values:[ 56 55 54 ] Children:[ 0 0 0 0 ] Leaf:t Prev:10 Next:12)"),
            s(b"(12 Keys:[ 57 58 59 ] Values:[ 59 58 57 ] Children:[ 0 0 0 0 ] Leaf:t Prev:11 Next:13)"),
            s(b"(13 Keys:[ 66 67 68 ] Values:[ 68 67 66 ] Children:[ 0 0 0 0 ] Leaf:t Prev:12 Next:14)"),
            s(b"(14 Keys:[ 76 77 78 ] Values:[ 78 77 76 ] Children:[ 0 0 0 0 ] Leaf:t Prev:13 Next:15)"),
            s(b"(15 Keys:[ 86 87 88 ] Values:[ 88 87 86 ] Children:[ 0 0 0 0 ] Leaf:t Prev:14 Next:16)"),
            s(b"(16 Keys:[ 96 97 98 ] Values:[ 98 97 96 ] Children:[ 0 0 0 0 ] Leaf:t Prev:15 Next:17)"),
            s(b"(17 Keys:[ 101 102 103 ] Values:[ 103 102 101 ] Children:[ 0 0 0 0 ] Leaf:t Prev:16 Next:18)"),
            s(b"(18 Keys:[ 109 110 111 ] Values:[ 111 110 109 ] Children:[ 0 0 0 0 ] Leaf:t Prev:17 Next:19)"),
            s(b"(19 Keys:[ 121 122 123 ] Values:[ 123 122 121 ] Children:[ 0 0 0 0 ] Leaf:t Prev:18 Next:0)"),
        ]);
        assert_valid_tree(&tree);
        let expectedIterationKeys = vector[
            5, 6, 7,
            11, 12, 13,
            41, 42, 43,
            54, 55, 56,
            57, 58, 59,
            66, 67, 68,
            76, 77, 78,
            86, 87, 88,
            96, 97, 98,
            101, 102, 103,
            109, 110, 111,
            121, 122, 123,
        ];
        let expectedIterationValues = vector[
            7, 6, 5,
            13, 12, 11,
            43, 42, 41,
            56, 55, 54,
            59, 58, 57,
            68, 67, 66,
            78, 77, 76,
            88, 87, 86,
            98, 97, 96,
            103, 102, 101,
            111, 110, 109,
            123, 122, 121,
        ];
        let it = tree_iterate(&tree, INCREASING_ITERATOR);
        let i = 0;
        while (it.pos.nodeID != 0) {
            let (key, val) = tree_get_next(&tree, &mut it);
            assert!(*vector::borrow(&expectedIterationKeys, i) == key, 0);
            assert!(vector::borrow(&expectedIterationValues, i) == val, 0);
            i = i + 1;
        };
        destroy_tree(tree);
    }

    #[test]
    fun test_tree_iterate_decreasing() {
        // Tests that iteration works in the decreasing direction.

        let tree = tree_from_strs(4, vector[
            s(b"(3 Keys:[ 50 70 100 ] Children:[ 4 5 6 7 ] Leaf:f Prev:0 Next:0)"),
            s(b"(4 Keys:[ 10 40 ] Children:[ 8 9 10 ] Leaf:f Prev:0 Next:0)"),
            s(b"(5 Keys:[ 56 65 ] Children:[ 11 12 13 ] Leaf:f Prev:0 Next:0)"),
            s(b"(6 Keys:[ 85 95 ] Children:[ 14 15 16 ] Leaf:f Prev:0 Next:0)"),
            s(b"(7 Keys:[ 105 120 ] Children:[ 17 18 19 ] Leaf:f Prev:0 Next:0)"),

            s(b"(8 Keys:[ 5 6 7 ] Values:[ 7 6 5 ] Children:[ 0 0 0 0 ] Leaf:t Prev:0 Next:9)"),
            s(b"(9 Keys:[ 11 12 13 ] Values:[ 13 12 11 ] Children:[ 0 0 0 0 ] Leaf:t Prev:8 Next:10)"),
            s(b"(10 Keys:[ 41 42 43 ] Values:[ 43 42 41 ] Children:[ 0 0 0 0 ] Leaf:t Prev:9 Next:11)"),
            s(b"(11 Keys:[ 54 55 56 ] Values:[ 56 55 54 ] Children:[ 0 0 0 0 ] Leaf:t Prev:10 Next:12)"),
            s(b"(12 Keys:[ 57 58 59 ] Values:[ 59 58 57 ] Children:[ 0 0 0 0 ] Leaf:t Prev:11 Next:13)"),
            s(b"(13 Keys:[ 66 67 68 ] Values:[ 68 67 66 ] Children:[ 0 0 0 0 ] Leaf:t Prev:12 Next:14)"),
            s(b"(14 Keys:[ 76 77 78 ] Values:[ 78 77 76 ] Children:[ 0 0 0 0 ] Leaf:t Prev:13 Next:15)"),
            s(b"(15 Keys:[ 86 87 88 ] Values:[ 88 87 86 ] Children:[ 0 0 0 0 ] Leaf:t Prev:14 Next:16)"),
            s(b"(16 Keys:[ 96 97 98 ] Values:[ 98 97 96 ] Children:[ 0 0 0 0 ] Leaf:t Prev:15 Next:17)"),
            s(b"(17 Keys:[ 101 102 103 ] Values:[ 103 102 101 ] Children:[ 0 0 0 0 ] Leaf:t Prev:16 Next:18)"),
            s(b"(18 Keys:[ 109 110 111 ] Values:[ 111 110 109 ] Children:[ 0 0 0 0 ] Leaf:t Prev:17 Next:19)"),
            s(b"(19 Keys:[ 121 122 123 ] Values:[ 123 122 121 ] Children:[ 0 0 0 0 ] Leaf:t Prev:18 Next:0)"),
        ]);
        assert_valid_tree(&tree);
        let expectedIterationKeys = vector[
            123, 122, 121,
            111, 110, 109,
            103, 102, 101,
            98, 97, 96,
            88, 87, 86,
            78, 77, 76,
            68, 67, 66,
            59, 58, 57,
            56, 55, 54,
            43, 42, 41,
            13, 12, 11,
            7, 6, 5,
        ];
        let expectedIterationValues = vector[
            121, 122, 123,
            109, 110, 111,
            101, 102, 103,
            96, 97, 98,
            86, 87, 88,
            76, 77, 78,
            66, 67, 68,
            57, 58, 59,
            54, 55, 56,
            41, 42, 43,
            11, 12, 13,
            5, 6, 7,
        ];
        let it = tree_iterate(&tree, DECREASING_ITERATOR);
        let i = 0;
        while (it.pos.nodeID != 0) {
            let (key, val) = tree_get_next(&tree, &mut it);
            assert!(*vector::borrow(&expectedIterationKeys, i) == key, 0);
            assert!(vector::borrow(&expectedIterationValues, i) == val, 0);
            i = i + 1;
        };
        destroy_tree(tree);
    }

    #[test]
    fun test_tree_reuse_nodes() {
        // Tests that the tree reuses nodes instead of creating new ones.

        let tree = build_tree(4, vector[1, 5, 6, 7, 8], vector[8, 7, 6, 5, 1]);
        assert!(tree.currNodeID == 4, 0);
        assert_tree(&tree, vector[
            s(b"(3 Keys:[ 6 ] Children:[ 1 2 ] Leaf:f Prev:0 Next:0)"),
            s(b"(1 Keys:[ 1 5 6 ] Values:[ 8 7 6 ] Children:[ 0 0 0 0 ] Leaf:t Prev:0 Next:2)"),
            s(b"(2 Keys:[ 7 8 ] Values:[ 5 1 ] Children:[ 0 0 0 ] Leaf:t Prev:1 Next:0)"),
        ]);
        // Delete and assert deleted nodes were added to the reuse stack.
        tree_delete(&mut tree, 5);
        tree_delete(&mut tree, 6);
        tree_delete(&mut tree, 7);
        assert_tree(&tree, vector[
            s(b"(1 Keys:[ 1 8 ] Values:[ 8 1 ] Children:[ 0 0 0 ] Leaf:t Prev:0 Next:0)"),
        ]);
        assert!(tree.unusedNodeStack == 3, 0);
        let unusedTop = table::borrow(&tree.nodes, 3);
        assert!(unusedTop.next == 2, 0);
        let unusedBottom = table::borrow(&tree.nodes, 2);
        assert!(unusedBottom.next == 0, 0);
        // Add new elements and assert deleted nodes were used.
        tree_insert(&mut tree, 5, 5);
        tree_insert(&mut tree, 6, 1);
        tree_insert(&mut tree, 7, 8);
        assert_tree(&tree, vector[
            s(b"(2 Keys:[ 6 ] Children:[ 1 3 ] Leaf:f Prev:0 Next:0)"),
            s(b"(1 Keys:[ 1 5 6 ] Values:[ 8 5 1 ] Children:[ 0 0 0 0 ] Leaf:t Prev:0 Next:3)"),
            s(b"(3 Keys:[ 7 8 ] Values:[ 8 1 ] Children:[ 0 0 0 ] Leaf:t Prev:1 Next:0)"),
        ]);
        assert!(tree.currNodeID == 4, 0);
        assert!(tree.unusedNodeStack == 0, 0);
        destroy_tree(tree);
    }

    #[test]
    fun test_tree_prealloc_nodes() {
        // Tests that the tree uses preallocated nodes instead of creating new ones.

        let tree = new_tree(4);
        prealloc_tree_nodes(&mut tree, 100);
        assert!(tree.currNodeID == 101, 0);
        assert!(tree.unusedNodeStack == 100, 0);
        assert_tree(&tree, vector[]);
        // Add new elements and assert no new nodes were used.
        tree_insert(&mut tree, 1, 6);
        tree_insert(&mut tree, 5, 5);
        tree_insert(&mut tree, 6, 1);
        tree_insert(&mut tree, 7, 8);
        tree_insert(&mut tree, 8, 7);
        assert_tree(&tree, vector[
            s(b"(98 Keys:[ 6 ] Children:[ 100 99 ] Leaf:f Prev:0 Next:0)"),
            s(b"(100 Keys:[ 1 5 6 ] Values:[ 6 5 1 ] Children:[ 0 0 0 0 ] Leaf:t Prev:0 Next:99)"),
            s(b"(99 Keys:[ 7 8 ] Values:[ 8 7 ] Children:[ 0 0 0 ] Leaf:t Prev:100 Next:0)"),
        ]);
        assert!(tree.currNodeID == 101, 0);
        assert!(tree.unusedNodeStack == 97, 0);
        destroy_tree(tree);
    }

    #[test]
    fun test_tree_insert_empty() {
        // Tests that elements can be added to an empty tree.

        let tree = new_tree(4);
        tree_insert(&mut tree, 1, 1);
        tree_insert(&mut tree, 10, 10);
        tree_insert(&mut tree, 5, 5);
        assert_valid_tree(&tree);
        assert_contains_keys(&tree, vector[1, 10, 5]);
        destroy_tree(tree);
    }

    #[test]
    fun test_tree_insert_into_root() {
        // Tests that elements can be added to the root node, without it splitting.

        let tree = tree_from_strs(8, vector[
            s(b"(3 Keys:[ 5 10 25 45 76 90 ] Values:[ 6 12 32 57 97 115 ] Children:[ 0 0 0 0 0 0 0 ] Leaf:t Prev:0 Next:0)")
        ]);
        assert_valid_tree(&tree);
        tree_insert(&mut tree, 1, 1);
        assert_valid_tree(&tree);
        assert_tree(&tree, vector[
            s(b"(3 Keys:[ 1 5 10 25 45 76 90 ] Values:[ 1 6 12 32 57 97 115 ] Children:[ 0 0 0 0 0 0 0 0 ] Leaf:t Prev:0 Next:0)")
        ]);
        assert_contains_keys(&tree, vector[1, 5, 10, 25, 45, 76, 90]);
        destroy_tree(tree);
    }

    #[test]
    fun test_tree_insert_into_root_increase_depth() {
        // Tests that elements can be added to the root node, causing it to increase the tree's depth.

        let tree = tree_from_strs(8, vector[
            s(b"(3 Keys:[ 1 2 5 10 25 45 76 90 ] Values:[ 1 2 6 12 32 57 97 115 ] Children:[ 0 0 0 0 0 0 0 0 0 ] Leaf:t Prev:0 Next:0)")
        ]);
        assert_valid_tree(&tree);
        tree_insert(&mut tree, 4, 5);
        assert_valid_tree(&tree);
        assert_tree(&tree, vector[
            s(b"(5 Keys:[ 10 ] Children:[ 3 4 ] Leaf:f Prev:0 Next:0)"),

            s(b"(3 Keys:[ 1 2 4 5 10 ] Values:[ 1 2 5 6 12 ] Children:[ 0 0 0 0 0 0 ] Leaf:t Prev:0 Next:4)"),
            s(b"(4 Keys:[ 25 45 76 90 ] Values:[ 32 57 97 115 ] Children:[ 0 0 0 0 0 ] Leaf:t Prev:3 Next:0)"),
        ]);
        assert_contains_keys(&tree, vector[1, 2, 4, 5, 10, 25, 45, 76, 90]);
        destroy_tree(tree);
    }

    #[test]
    fun test_tree_insert_non_root_increase_depth_end_of_node() {
        // Tests that elements can be added to a node at a non root level, causing it to increase the tree's depth.
        // The element inserted should be inserted to the end of the node.

        let tree = tree_from_strs(8, vector[
            s(b"(1 Keys:[ 10 20 30 40 50 60 70 80 ] Children:[ 2 3 4 5 6 7 8 9 10 ] Leaf:f Prev:0 Next:0)"),

            s(b"(2 Keys:[ 1 2 4 5 6 7 10 ] Values:[ 1 2 5 6 7 9 12 ] Children:[ 0 0 0 0 0 0 0 0 ] Leaf:t Prev:0 Next:3)"),
            s(b"(3 Keys:[ 11 12 14 15 16 17 20 ] Values:[ 14 15 18 19 20 21 25 ] Children:[ 0 0 0 0 0 0 0 0 ] Leaf:t Prev:2 Next:4)"),
            s(b"(4 Keys:[ 21 22 24 25 26 27 30 ] Values:[ 27 28 30 32 33 34 38 ] Children:[ 0 0 0 0 0 0 0 0 ] Leaf:t Prev:3 Next:5)"),
            s(b"(5 Keys:[ 31 32 34 35 36 37 39 ] Values:[ 39 41 43 45 46 47 50 ] Children:[ 0 0 0 0 0 0 0 0 ] Leaf:t Prev:4 Next:6)"),
            s(b"(6 Keys:[ 41 42 44 45 46 47 50 ] Values:[ 52 54 56 57 59 60 64 ] Children:[ 0 0 0 0 0 0 0 0 ] Leaf:t Prev:5 Next:7)"),
            s(b"(7 Keys:[ 51 52 54 55 56 57 60 ] Values:[ 65 66 69 70 72 73 77 ] Children:[ 0 0 0 0 0 0 0 0 ] Leaf:t Prev:6 Next:8)"),
            s(b"(8 Keys:[ 61 62 64 65 70 ] Values:[ 78 79 82 83 90 ] Children:[ 0 0 0 0 0 0 ] Leaf:t Prev:7 Next:9)"),
            s(b"(9 Keys:[ 71 72 74 80 ] Values:[ 91 92 95 102 ] Children:[ 0 0 0 0 0 ] Leaf:t Prev:8 Next:10)"),
            s(b"(10 Keys:[ 81 82 83 84 85 ] Values:[ 104 105 106 108 109 ] Children:[ 0 0 0 0 0 0 ] Leaf:t Prev:9 Next:0)"),
        ]);
        assert_valid_tree(&tree);
        tree_insert(&mut tree, 40, 51);
        assert_valid_tree(&tree);
        assert_tree(&tree, vector[
            s(b"(12 Keys:[ 50 ] Children:[ 1 11 ] Leaf:f Prev:0 Next:0)"),
            s(b"(1 Keys:[ 10 20 30 40 ] Children:[ 2 3 4 5 6 ] Leaf:f Prev:0 Next:0)"),
            s(b"(11 Keys:[ 60 70 80 ] Children:[ 7 8 9 10 ] Leaf:f Prev:0 Next:0)"),

            s(b"(2 Keys:[ 1 2 4 5 6 7 10 ] Values:[ 1 2 5 6 7 9 12 ] Children:[ 0 0 0 0 0 0 0 0 ] Leaf:t Prev:0 Next:3)"),
            s(b"(3 Keys:[ 11 12 14 15 16 17 20 ] Values:[ 14 15 18 19 20 21 25 ] Children:[ 0 0 0 0 0 0 0 0 ] Leaf:t Prev:2 Next:4)"),
            s(b"(4 Keys:[ 21 22 24 25 26 27 30 ] Values:[ 27 28 30 32 33 34 38 ] Children:[ 0 0 0 0 0 0 0 0 ] Leaf:t Prev:3 Next:5)"),
            s(b"(5 Keys:[ 31 32 34 35 36 37 39 40 ] Values:[ 39 41 43 45 46 47 50 51 ] Children:[ 0 0 0 0 0 0 0 0 0 ] Leaf:t Prev:4 Next:6)"),
            s(b"(6 Keys:[ 41 42 44 45 46 47 50 ] Values:[ 52 54 56 57 59 60 64 ] Children:[ 0 0 0 0 0 0 0 0 ] Leaf:t Prev:5 Next:7)"),
            s(b"(7 Keys:[ 51 52 54 55 56 57 60 ] Values:[ 65 66 69 70 72 73 77 ] Children:[ 0 0 0 0 0 0 0 0 ] Leaf:t Prev:6 Next:8)"),
            s(b"(8 Keys:[ 61 62 64 65 70 ] Values:[ 78 79 82 83 90 ] Children:[ 0 0 0 0 0 0 ] Leaf:t Prev:7 Next:9)"),
            s(b"(9 Keys:[ 71 72 74 80 ] Values:[ 91 92 95 102 ] Children:[ 0 0 0 0 0 ] Leaf:t Prev:8 Next:10)"),
            s(b"(10 Keys:[ 81 82 83 84 85 ] Values:[ 104 105 106 108 109 ] Children:[ 0 0 0 0 0 0 ] Leaf:t Prev:9 Next:0)"),
        ]);
        assert_contains_keys(&tree, vector[
            1, 2, 4, 5, 6, 7, 10, 11, 12, 14, 15, 16, 17, 20, 21, 22, 24, 25, 26, 27, 30,
            31, 32, 34, 35, 36, 37, 39, 40, 41, 42, 44, 45, 46, 47, 50, 51, 52, 54, 55, 56,
            57, 60, 61, 62, 64, 65, 70, 71, 72, 74, 80, 81, 82, 83, 84, 85
        ]);
        destroy_tree(tree);
    }

    #[test]
    fun test_tree_insert_non_root_no_increase_depth_end_of_node() {
        // Tests that elements can be added to a node at a non root level, causing it to split but not
        // increase the tree's depth. The element inserted should be inserted to the end of the node.

        let tree = tree_from_strs(8, vector[
            s(b"(1 Keys:[ 10 20 30 40 50 60 70 ] Children:[ 2 3 4 5 6 7 8 9 ] Leaf:f Prev:0 Next:0)"),

            s(b"(2 Keys:[ 1 2 4 5 6 7 10 ] Values:[ 1 2 5 6 7 9 12 ] Children:[ 0 0 0 0 0 0 0 0 ] Leaf:t Prev:0 Next:3)"),
            s(b"(3 Keys:[ 11 12 14 15 16 17 20 ] Values:[ 14 15 18 19 20 21 25 ] Children:[ 0 0 0 0 0 0 0 0 ] Leaf:t Prev:2 Next:4)"),
            s(b"(4 Keys:[ 21 22 24 25 26 27 30 ] Values:[ 27 28 30 32 33 34 38 ] Children:[ 0 0 0 0 0 0 0 0 ] Leaf:t Prev:3 Next:5)"),
            s(b"(5 Keys:[ 31 32 34 35 36 37 39 ] Values:[ 39 41 43 45 46 47 50 ] Children:[ 0 0 0 0 0 0 0 0 ] Leaf:t Prev:4 Next:6)"),
            s(b"(6 Keys:[ 41 42 44 45 46 47 50 ] Values:[ 52 54 56 57 59 60 64 ] Children:[ 0 0 0 0 0 0 0 0 ] Leaf:t Prev:5 Next:7)"),
            s(b"(7 Keys:[ 51 52 54 55 56 57 60 ] Values:[ 65 66 69 70 72 73 77 ] Children:[ 0 0 0 0 0 0 0 0 ] Leaf:t Prev:6 Next:8)"),
            s(b"(8 Keys:[ 61 62 63 64 65 67 68 69 ] Values:[ 78 79 81 82 83 86 87 88 ] Children:[ 0 0 0 0 0 0 0 0 0 ] Leaf:t Prev:7 Next:9)"),
            s(b"(9 Keys:[ 71 72 74 80 ] Values:[ 91 92 95 102 ] Children:[ 0 0 0 0 0 ] Leaf:t Prev:8 Next:0)"),
        ]);
        assert_valid_tree(&tree);
        tree_insert(&mut tree, 70, 90);
        assert_valid_tree(&tree);
        assert_tree(&tree, vector[
            s(b"(1 Keys:[ 10 20 30 40 50 60 65 70 ] Children:[ 2 3 4 5 6 7 8 10 9 ] Leaf:f Prev:0 Next:0)"),

            s(b"(2 Keys:[ 1 2 4 5 6 7 10 ] Values:[ 1 2 5 6 7 9 12 ] Children:[ 0 0 0 0 0 0 0 0 ] Leaf:t Prev:0 Next:3)"),
            s(b"(3 Keys:[ 11 12 14 15 16 17 20 ] Values:[ 14 15 18 19 20 21 25 ] Children:[ 0 0 0 0 0 0 0 0 ] Leaf:t Prev:2 Next:4)"),
            s(b"(4 Keys:[ 21 22 24 25 26 27 30 ] Values:[ 27 28 30 32 33 34 38 ] Children:[ 0 0 0 0 0 0 0 0 ] Leaf:t Prev:3 Next:5)"),
            s(b"(5 Keys:[ 31 32 34 35 36 37 39 ] Values:[ 39 41 43 45 46 47 50 ] Children:[ 0 0 0 0 0 0 0 0 ] Leaf:t Prev:4 Next:6)"),
            s(b"(6 Keys:[ 41 42 44 45 46 47 50 ] Values:[ 52 54 56 57 59 60 64 ] Children:[ 0 0 0 0 0 0 0 0 ] Leaf:t Prev:5 Next:7)"),
            s(b"(7 Keys:[ 51 52 54 55 56 57 60 ] Values:[ 65 66 69 70 72 73 77 ] Children:[ 0 0 0 0 0 0 0 0 ] Leaf:t Prev:6 Next:8)"),
            s(b"(8 Keys:[ 61 62 63 64 65 ] Values:[ 78 79 81 82 83 ] Children:[ 0 0 0 0 0 0 ] Leaf:t Prev:7 Next:10)"),
            s(b"(10 Keys:[ 67 68 69 70 ] Values:[ 86 87 88 90 ] Children:[ 0 0 0 0 0 ] Leaf:t Prev:8 Next:9)"),
            s(b"(9 Keys:[ 71 72 74 80 ] Values:[ 91 92 95 102 ] Children:[ 0 0 0 0 0 ] Leaf:t Prev:10 Next:0)"),
        ]);
        assert_contains_keys(&tree, vector[
            1, 2, 4, 5, 6, 7, 10, 11, 12, 14, 15, 16, 17, 20, 21, 22, 24, 25, 26, 27, 30,
            31, 32, 34, 35, 36, 37, 39, 41, 42, 44, 45, 46, 47, 50, 51, 52, 54, 55, 56,
            57, 60, 61, 62, 63, 64, 65, 67, 68, 69, 70, 71, 72, 74, 80
        ]);
        destroy_tree(tree);
    }

    #[test]
    fun test_tree_insert_non_root_increase_depth_start_of_node() {
        // Tests that elements can be added to a node at a non root level, causing it to increase the tree's depth.
        // The element inserted should be inserted to the start of the node.

        let tree = tree_from_strs(8, vector[
            s(b"(1 Keys:[ 10 20 30 40 50 60 70 80 ] Children:[ 2 3 4 5 6 7 8 9 10 ] Leaf:f Prev:0 Next:0)"),

            s(b"(2 Keys:[ 2 3 4 5 6 7 10 ] Values:[ 2 3 5 6 7 9 12 ] Children:[ 0 0 0 0 0 0 0 0 ] Leaf:t Prev:0 Next:3)"),
            s(b"(3 Keys:[ 11 12 14 15 16 17 20 ] Values:[ 14 15 18 19 20 21 25 ] Children:[ 0 0 0 0 0 0 0 0 ] Leaf:t Prev:2 Next:4)"),
            s(b"(4 Keys:[ 21 22 24 25 26 27 30 ] Values:[ 27 28 30 32 33 34 38 ] Children:[ 0 0 0 0 0 0 0 0 ] Leaf:t Prev:3 Next:5)"),
            s(b"(5 Keys:[ 31 32 34 35 36 37 39 ] Values:[ 39 41 43 45 46 47 50 ] Children:[ 0 0 0 0 0 0 0 0 ] Leaf:t Prev:4 Next:6)"),
            s(b"(6 Keys:[ 41 42 44 45 46 47 50 ] Values:[ 52 54 56 57 59 60 64 ] Children:[ 0 0 0 0 0 0 0 0 ] Leaf:t Prev:5 Next:7)"),
            s(b"(7 Keys:[ 51 52 54 55 56 57 60 ] Values:[ 65 66 69 70 72 73 77 ] Children:[ 0 0 0 0 0 0 0 0 ] Leaf:t Prev:6 Next:8)"),
            s(b"(8 Keys:[ 61 62 64 65 70 ] Values:[ 78 79 82 83 90 ] Children:[ 0 0 0 0 0 0 ] Leaf:t Prev:7 Next:9)"),
            s(b"(9 Keys:[ 71 72 74 80 ] Values:[ 91 92 95 102 ] Children:[ 0 0 0 0 0 ] Leaf:t Prev:8 Next:10)"),
            s(b"(10 Keys:[ 81 82 83 84 85 ] Values:[ 104 105 106 108 109 ] Children:[ 0 0 0 0 0 0 ] Leaf:t Prev:9 Next:0)"),
        ]);
        assert_valid_tree(&tree);
        tree_insert(&mut tree, 1, 1);
        assert_valid_tree(&tree);
        assert_tree(&tree, vector[
            s(b"(12 Keys:[ 50 ] Children:[ 1 11 ] Leaf:f Prev:0 Next:0)"),
            s(b"(1 Keys:[ 10 20 30 40 ] Children:[ 2 3 4 5 6 ] Leaf:f Prev:0 Next:0)"),
            s(b"(11 Keys:[ 60 70 80 ] Children:[ 7 8 9 10 ] Leaf:f Prev:0 Next:0)"),

            s(b"(2 Keys:[ 1 2 3 4 5 6 7 10 ] Values:[ 1 2 3 5 6 7 9 12 ] Children:[ 0 0 0 0 0 0 0 0 0 ] Leaf:t Prev:0 Next:3)"),
            s(b"(3 Keys:[ 11 12 14 15 16 17 20 ] Values:[ 14 15 18 19 20 21 25 ] Children:[ 0 0 0 0 0 0 0 0 ] Leaf:t Prev:2 Next:4)"),
            s(b"(4 Keys:[ 21 22 24 25 26 27 30 ] Values:[ 27 28 30 32 33 34 38 ] Children:[ 0 0 0 0 0 0 0 0 ] Leaf:t Prev:3 Next:5)"),
            s(b"(5 Keys:[ 31 32 34 35 36 37 39 ] Values:[ 39 41 43 45 46 47 50 ] Children:[ 0 0 0 0 0 0 0 0 ] Leaf:t Prev:4 Next:6)"),
            s(b"(6 Keys:[ 41 42 44 45 46 47 50 ] Values:[ 52 54 56 57 59 60 64 ] Children:[ 0 0 0 0 0 0 0 0 ] Leaf:t Prev:5 Next:7)"),
            s(b"(7 Keys:[ 51 52 54 55 56 57 60 ] Values:[ 65 66 69 70 72 73 77 ] Children:[ 0 0 0 0 0 0 0 0 ] Leaf:t Prev:6 Next:8)"),
            s(b"(8 Keys:[ 61 62 64 65 70 ] Values:[ 78 79 82 83 90 ] Children:[ 0 0 0 0 0 0 ] Leaf:t Prev:7 Next:9)"),
            s(b"(9 Keys:[ 71 72 74 80 ] Values:[ 91 92 95 102 ] Children:[ 0 0 0 0 0 ] Leaf:t Prev:8 Next:10)"),
            s(b"(10 Keys:[ 81 82 83 84 85 ] Values:[ 104 105 106 108 109 ] Children:[ 0 0 0 0 0 0 ] Leaf:t Prev:9 Next:0)"),
        ]);
        assert_contains_keys(&tree, vector[
            1, 2, 3, 4, 5, 6, 7, 10,
            11, 12, 14, 15, 16, 17, 20,
            21, 22, 24, 25, 26, 27, 30,
            31, 32, 34, 35, 36, 37, 39,
            41, 42, 44, 45, 46, 47, 50,
            51, 52, 54, 55, 56, 57, 60,
            61, 62, 64, 65, 70,
            71, 72, 74, 80,
            81, 82, 83, 84, 85
        ]);
        destroy_tree(tree);
    }

    #[test]
    fun test_tree_insert_non_root_no_increase_depth_start_of_node() {
        // Tests that elements can be added to a node at a non root level, causing it to split but not
        // increase the tree's depth. The element inserted should be inserted to the start of the node.

        let tree = tree_from_strs(8, vector[
            s(b"(1 Keys:[ 10 20 30 40 50 60 70 ] Children:[ 2 3 4 5 6 7 8 9 ] Leaf:f Prev:0 Next:0)"),

            s(b"(2 Keys:[ 2 3 4 5 6 7 8 10 ] Values:[ 2 3 5 6 7 9 10 12 ] Children:[ 0 0 0 0 0 0 0 0 0 ] Leaf:t Prev:0 Next:3)"),
            s(b"(3 Keys:[ 11 12 14 15 16 17 20 ] Values:[ 14 15 18 19 20 21 25 ] Children:[ 0 0 0 0 0 0 0 0 ] Leaf:t Prev:2 Next:4)"),
            s(b"(4 Keys:[ 21 22 24 25 26 27 30 ] Values:[ 27 28 30 32 33 34 38 ] Children:[ 0 0 0 0 0 0 0 0 ] Leaf:t Prev:3 Next:5)"),
            s(b"(5 Keys:[ 31 32 34 35 36 37 39 ] Values:[ 39 41 43 45 46 47 50 ] Children:[ 0 0 0 0 0 0 0 0 ] Leaf:t Prev:4 Next:6)"),
            s(b"(6 Keys:[ 41 42 44 45 46 47 50 ] Values:[ 52 54 56 57 59 60 64 ] Children:[ 0 0 0 0 0 0 0 0 ] Leaf:t Prev:5 Next:7)"),
            s(b"(7 Keys:[ 51 52 54 55 56 57 60 ] Values:[ 65 66 69 70 72 73 77 ] Children:[ 0 0 0 0 0 0 0 0 ] Leaf:t Prev:6 Next:8)"),
            s(b"(8 Keys:[ 61 62 64 65 69 ] Values:[ 78 79 82 83 88 ] Children:[ 0 0 0 0 0 0 ] Leaf:t Prev:7 Next:9)"),
            s(b"(9 Keys:[ 71 72 74 80 ] Values:[ 91 92 95 102 ] Children:[ 0 0 0 0 0 ] Leaf:t Prev:8 Next:0)"),
        ]);
        assert_valid_tree(&tree);
        tree_insert(&mut tree, 1, 1);
        assert_valid_tree(&tree);
        assert_tree(&tree, vector[
            s(b"(1 Keys:[ 5 10 20 30 40 50 60 70 ] Children:[ 2 10 3 4 5 6 7 8 9 ] Leaf:f Prev:0 Next:0)"),

            s(b"(2 Keys:[ 1 2 3 4 5 ] Values:[ 1 2 3 5 6 ] Children:[ 0 0 0 0 0 0 ] Leaf:t Prev:0 Next:10)"),
            s(b"(10 Keys:[ 6 7 8 10 ] Values:[ 7 9 10 12 ] Children:[ 0 0 0 0 0 ] Leaf:t Prev:2 Next:3)"),
            s(b"(3 Keys:[ 11 12 14 15 16 17 20 ] Values:[ 14 15 18 19 20 21 25 ] Children:[ 0 0 0 0 0 0 0 0 ] Leaf:t Prev:10 Next:4)"),
            s(b"(4 Keys:[ 21 22 24 25 26 27 30 ] Values:[ 27 28 30 32 33 34 38 ] Children:[ 0 0 0 0 0 0 0 0 ] Leaf:t Prev:3 Next:5)"),
            s(b"(5 Keys:[ 31 32 34 35 36 37 39 ] Values:[ 39 41 43 45 46 47 50 ] Children:[ 0 0 0 0 0 0 0 0 ] Leaf:t Prev:4 Next:6)"),
            s(b"(6 Keys:[ 41 42 44 45 46 47 50 ] Values:[ 52 54 56 57 59 60 64 ] Children:[ 0 0 0 0 0 0 0 0 ] Leaf:t Prev:5 Next:7)"),
            s(b"(7 Keys:[ 51 52 54 55 56 57 60 ] Values:[ 65 66 69 70 72 73 77 ] Children:[ 0 0 0 0 0 0 0 0 ] Leaf:t Prev:6 Next:8)"),
            s(b"(8 Keys:[ 61 62 64 65 69 ] Values:[ 78 79 82 83 88 ] Children:[ 0 0 0 0 0 0 ] Leaf:t Prev:7 Next:9)"),
            s(b"(9 Keys:[ 71 72 74 80 ] Values:[ 91 92 95 102 ] Children:[ 0 0 0 0 0 ] Leaf:t Prev:8 Next:0)"),
        ]);
        assert_contains_keys(&tree, vector[
            1, 2, 3, 4, 5,
            6, 7, 8, 10,
            11, 12, 14, 15, 16, 17, 20,
            21, 22, 24, 25, 26, 27, 30,
            31, 32, 34, 35, 36, 37, 39,
            41, 42, 44, 45, 46, 47, 50,
            51, 52, 54, 55, 56, 57, 60,
            61, 62, 64, 65, 69,
            71, 72, 74, 80,
        ]);
        destroy_tree(tree);
    }

    #[test]
    fun test_tree_insert_non_root_increase_depth_middle_of_node() {
        // Tests that elements can be added to a node at a non root level, causing it to increase the tree's depth.
        // The element inserted should be inserted to the middle of the node.

        let tree = tree_from_strs(8, vector[
            s(b"(1 Keys:[ 10 20 30 40 50 60 70 80 ] Children:[ 2 3 4 5 6 7 8 9 10 ] Leaf:f Prev:0 Next:0)"),

            s(b"(2 Keys:[ 1 2 3 4 6 7 10 ] Values:[ 1 2 3 5 7 9 12 ] Children:[ 0 0 0 0 0 0 0 0 ] Leaf:t Prev:0 Next:3)"),
            s(b"(3 Keys:[ 11 12 14 15 16 17 20 ] Values:[ 14 15 18 19 20 21 25 ] Children:[ 0 0 0 0 0 0 0 0 ] Leaf:t Prev:2 Next:4)"),
            s(b"(4 Keys:[ 21 22 24 25 26 27 30 ] Values:[ 27 28 30 32 33 34 38 ] Children:[ 0 0 0 0 0 0 0 0 ] Leaf:t Prev:3 Next:5)"),
            s(b"(5 Keys:[ 31 32 34 35 36 37 39 ] Values:[ 39 41 43 45 46 47 50 ] Children:[ 0 0 0 0 0 0 0 0 ] Leaf:t Prev:4 Next:6)"),
            s(b"(6 Keys:[ 41 42 44 45 46 47 50 ] Values:[ 52 54 56 57 59 60 64 ] Children:[ 0 0 0 0 0 0 0 0 ] Leaf:t Prev:5 Next:7)"),
            s(b"(7 Keys:[ 51 52 54 55 56 57 60 ] Values:[ 65 66 69 70 72 73 77 ] Children:[ 0 0 0 0 0 0 0 0 ] Leaf:t Prev:6 Next:8)"),
            s(b"(8 Keys:[ 61 62 64 65 70 ] Values:[ 78 79 82 83 90 ] Children:[ 0 0 0 0 0 0 ] Leaf:t Prev:7 Next:9)"),
            s(b"(9 Keys:[ 71 72 74 80 ] Values:[ 91 92 95 102 ] Children:[ 0 0 0 0 0 ] Leaf:t Prev:8 Next:10)"),
            s(b"(10 Keys:[ 81 82 83 84 85 ] Values:[ 104 105 106 108 109 ] Children:[ 0 0 0 0 0 0 ] Leaf:t Prev:9 Next:0)"),
        ]);
        assert_valid_tree(&tree);
        tree_insert(&mut tree, 5, 6);
        assert_valid_tree(&tree);
        assert_tree(&tree, vector[
            s(b"(12 Keys:[ 50 ] Children:[ 1 11 ] Leaf:f Prev:0 Next:0)"),
            s(b"(1 Keys:[ 10 20 30 40 ] Children:[ 2 3 4 5 6 ] Leaf:f Prev:0 Next:0)"),
            s(b"(11 Keys:[ 60 70 80 ] Children:[ 7 8 9 10 ] Leaf:f Prev:0 Next:0)"),

            s(b"(2 Keys:[ 1 2 3 4 5 6 7 10 ] Values:[ 1 2 3 5 6 7 9 12 ] Children:[ 0 0 0 0 0 0 0 0 0 ] Leaf:t Prev:0 Next:3)"),
            s(b"(3 Keys:[ 11 12 14 15 16 17 20 ] Values:[ 14 15 18 19 20 21 25 ] Children:[ 0 0 0 0 0 0 0 0 ] Leaf:t Prev:2 Next:4)"),
            s(b"(4 Keys:[ 21 22 24 25 26 27 30 ] Values:[ 27 28 30 32 33 34 38 ] Children:[ 0 0 0 0 0 0 0 0 ] Leaf:t Prev:3 Next:5)"),
            s(b"(5 Keys:[ 31 32 34 35 36 37 39 ] Values:[ 39 41 43 45 46 47 50 ] Children:[ 0 0 0 0 0 0 0 0 ] Leaf:t Prev:4 Next:6)"),
            s(b"(6 Keys:[ 41 42 44 45 46 47 50 ] Values:[ 52 54 56 57 59 60 64 ] Children:[ 0 0 0 0 0 0 0 0 ] Leaf:t Prev:5 Next:7)"),
            s(b"(7 Keys:[ 51 52 54 55 56 57 60 ] Values:[ 65 66 69 70 72 73 77 ] Children:[ 0 0 0 0 0 0 0 0 ] Leaf:t Prev:6 Next:8)"),
            s(b"(8 Keys:[ 61 62 64 65 70 ] Values:[ 78 79 82 83 90 ] Children:[ 0 0 0 0 0 0 ] Leaf:t Prev:7 Next:9)"),
            s(b"(9 Keys:[ 71 72 74 80 ] Values:[ 91 92 95 102 ] Children:[ 0 0 0 0 0 ] Leaf:t Prev:8 Next:10)"),
            s(b"(10 Keys:[ 81 82 83 84 85 ] Values:[ 104 105 106 108 109 ] Children:[ 0 0 0 0 0 0 ] Leaf:t Prev:9 Next:0)"),
        ]);
        assert_contains_keys(&tree, vector[
            1, 2, 3, 4, 5, 6, 7, 10,
            11, 12, 14, 15, 16, 17, 20,
            21, 22, 24, 25, 26, 27, 30,
            31, 32, 34, 35, 36, 37, 39,
            41, 42, 44, 45, 46, 47, 50,
            51, 52, 54, 55, 56, 57, 60,
            61, 62, 64, 65, 70,
            71, 72, 74, 80,
            81, 82, 83, 84, 85
        ]);
        destroy_tree(tree);
    }

    #[test]
    fun test_tree_insert_non_root_no_increase_depth_middle_of_node() {
        // Tests that elements can be added to a node at a non root level, causing it to split but not
        // increase the tree's depth. The element inserted should be inserted to the middle of the node.

        let tree = tree_from_strs(8, vector[
            s(b"(1 Keys:[ 10 20 30 40 50 60 70 ] Children:[ 2 3 4 5 6 7 8 9 ] Leaf:f Prev:0 Next:0)"),

            s(b"(2 Keys:[ 2 3 4 5 6 7 10 ] Values:[ 2 3 5 6 7 9 12 ] Children:[ 0 0 0 0 0 0 0 0 ] Leaf:t Prev:0 Next:3)"),
            s(b"(3 Keys:[ 11 12 14 15 16 17 20 ] Values:[ 14 15 18 19 20 21 25 ] Children:[ 0 0 0 0 0 0 0 0 ] Leaf:t Prev:2 Next:4)"),
            s(b"(4 Keys:[ 21 22 24 25 26 27 30 ] Values:[ 27 28 30 32 33 34 38 ] Children:[ 0 0 0 0 0 0 0 0 ] Leaf:t Prev:3 Next:5)"),
            s(b"(5 Keys:[ 31 32 34 35 36 37 39 ] Values:[ 39 41 43 45 46 47 50 ] Children:[ 0 0 0 0 0 0 0 0 ] Leaf:t Prev:4 Next:6)"),
            s(b"(6 Keys:[ 41 42 44 45 46 47 50 ] Values:[ 52 54 56 57 59 60 64 ] Children:[ 0 0 0 0 0 0 0 0 ] Leaf:t Prev:5 Next:7)"),
            s(b"(7 Keys:[ 51 52 54 55 56 57 59 60 ] Values:[ 65 66 69 70 72 73 75 77 ] Children:[ 0 0 0 0 0 0 0 0 0 ] Leaf:t Prev:6 Next:8)"),
            s(b"(8 Keys:[ 61 62 64 65 69 ] Values:[ 78 79 82 83 88 ] Children:[ 0 0 0 0 0 0 ] Leaf:t Prev:7 Next:9)"),
            s(b"(9 Keys:[ 71 72 74 80 ] Values:[ 91 92 95 102 ] Children:[ 0 0 0 0 0 ] Leaf:t Prev:8 Next:0)"),
        ]);
        assert_valid_tree(&tree);
        tree_insert(&mut tree, 58, 74);
        assert_valid_tree(&tree);
        assert_tree(&tree, vector[
            s(b"(1 Keys:[ 10 20 30 40 50 56 60 70 ] Children:[ 2 3 4 5 6 7 10 8 9 ] Leaf:f Prev:0 Next:0)"),

            s(b"(2 Keys:[ 2 3 4 5 6 7 10 ] Values:[ 2 3 5 6 7 9 12 ] Children:[ 0 0 0 0 0 0 0 0 ] Leaf:t Prev:0 Next:3)"),
            s(b"(3 Keys:[ 11 12 14 15 16 17 20 ] Values:[ 14 15 18 19 20 21 25 ] Children:[ 0 0 0 0 0 0 0 0 ] Leaf:t Prev:2 Next:4)"),
            s(b"(4 Keys:[ 21 22 24 25 26 27 30 ] Values:[ 27 28 30 32 33 34 38 ] Children:[ 0 0 0 0 0 0 0 0 ] Leaf:t Prev:3 Next:5)"),
            s(b"(5 Keys:[ 31 32 34 35 36 37 39 ] Values:[ 39 41 43 45 46 47 50 ] Children:[ 0 0 0 0 0 0 0 0 ] Leaf:t Prev:4 Next:6)"),
            s(b"(6 Keys:[ 41 42 44 45 46 47 50 ] Values:[ 52 54 56 57 59 60 64 ] Children:[ 0 0 0 0 0 0 0 0 ] Leaf:t Prev:5 Next:7)"),
            s(b"(7 Keys:[ 51 52 54 55 56 ] Values:[ 65 66 69 70 72 ] Children:[ 0 0 0 0 0 0 ] Leaf:t Prev:6 Next:10)"),
            s(b"(10 Keys:[ 57 58 59 60 ] Values:[ 73 74 75 77 ] Children:[ 0 0 0 0 0 ] Leaf:t Prev:7 Next:8)"),
            s(b"(8 Keys:[ 61 62 64 65 69 ] Values:[ 78 79 82 83 88 ] Children:[ 0 0 0 0 0 0 ] Leaf:t Prev:10 Next:9)"),
            s(b"(9 Keys:[ 71 72 74 80 ] Values:[ 91 92 95 102 ] Children:[ 0 0 0 0 0 ] Leaf:t Prev:8 Next:0)"),
        ]);
        assert_contains_keys(&tree, vector[
            2, 3, 4, 5, 6, 7, 10,
            11, 12, 14, 15, 16, 17, 20,
            21, 22, 24, 25, 26, 27, 30,
            31, 32, 34, 35, 36, 37, 39,
            41, 42, 44, 45, 46, 47, 50,
            51, 52, 54, 55, 56,
            57, 58, 59, 60,
            61, 62, 64, 65, 69,
            71, 72, 74, 80,
        ]);
        destroy_tree(tree);
    }

    #[test]
    fun test_tree_insert_internal_first_child_no_depth_increase() {
        // Tests that elements can be added to an internal node because of a leaf node split.

        let tree = tree_from_strs(4, vector[
            s(b"(3 Keys:[ 50 70 100 ] Children:[ 4 5 6 7 ] Leaf:f Prev:0 Next:0)"),
            s(b"(4 Keys:[ 10 40 ] Children:[ 8 9 10 ] Leaf:f Prev:0 Next:0)"),
            s(b"(5 Keys:[ 55 65 68 ] Children:[ 11 12 13 20 ] Leaf:f Prev:0 Next:0)"),
            s(b"(6 Keys:[ 85 ] Children:[ 15 16 ] Leaf:f Prev:0 Next:0)"),
            s(b"(7 Keys:[ 120 ] Children:[ 18 19 ] Leaf:f Prev:0 Next:0)"),

            s(b"(8 Keys:[ 1 2 5 6 ] Values:[ 1 2 6 7 ] Children:[ 0 0 0 0 0 ] Leaf:t Prev:0 Next:9)"),
            s(b"(9 Keys:[ 11 12 ] Values:[ 14 15 ] Children:[ 0 0 0 ] Leaf:t Prev:8 Next:10)"),
            s(b"(10 Keys:[ 41 42 ] Values:[ 52 54 ] Children:[ 0 0 0 ] Leaf:t Prev:9 Next:11)"),
            s(b"(11 Keys:[ 54 55 ] Values:[ 69 70 ] Children:[ 0 0 0 ] Leaf:t Prev:10 Next:12)"),
            s(b"(12 Keys:[ 56 57 ] Values:[ 72 73 ] Children:[ 0 0 0 ] Leaf:t Prev:11 Next:13)"),
            s(b"(13 Keys:[ 66 67 ] Values:[ 84 86 ] Children:[ 0 0 0 ] Leaf:t Prev:12 Next:20)"),
            s(b"(20 Keys:[ 69 70 ] Values:[ 88 90 ] Children:[ 0 0 0 ] Leaf:t Prev:13 Next:15)"),
            s(b"(15 Keys:[ 76 77 ] Values:[ 97 99 ] Children:[ 0 0 0 ] Leaf:t Prev:20 Next:16)"),
            s(b"(16 Keys:[ 86 87 ] Values:[ 110 111 ] Children:[ 0 0 0 ] Leaf:t Prev:15 Next:18)"),
            s(b"(18 Keys:[ 111 120 ] Values:[ 142 154 ] Children:[ 0 0 0 ] Leaf:t Prev:16 Next:19)"),
            s(b"(19 Keys:[ 121 122 ] Values:[ 155 156 ] Children:[ 0 0 0 ] Leaf:t Prev:18 Next:0)"),
        ]);
        assert_valid_tree(&tree);
        tree_insert(&mut tree, 3, 3);
        assert_valid_tree(&tree);
        assert_tree(&tree, vector[
            s(b"(3 Keys:[ 50 70 100 ] Children:[ 4 5 6 7 ] Leaf:f Prev:0 Next:0)"),
            s(b"(4 Keys:[ 3 10 40 ] Children:[ 8 21 9 10 ] Leaf:f Prev:0 Next:0)"),
            s(b"(5 Keys:[ 55 65 68 ] Children:[ 11 12 13 20 ] Leaf:f Prev:0 Next:0)"),
            s(b"(6 Keys:[ 85 ] Children:[ 15 16 ] Leaf:f Prev:0 Next:0)"),
            s(b"(7 Keys:[ 120 ] Children:[ 18 19 ] Leaf:f Prev:0 Next:0)"),

            s(b"(8 Keys:[ 1 2 3 ] Values:[ 1 2 3 ] Children:[ 0 0 0 0 ] Leaf:t Prev:0 Next:21)"),
            s(b"(21 Keys:[ 5 6 ] Values:[ 6 7 ] Children:[ 0 0 0 ] Leaf:t Prev:8 Next:9)"),
            s(b"(9 Keys:[ 11 12 ] Values:[ 14 15 ] Children:[ 0 0 0 ] Leaf:t Prev:21 Next:10)"),
            s(b"(10 Keys:[ 41 42 ] Values:[ 52 54 ] Children:[ 0 0 0 ] Leaf:t Prev:9 Next:11)"),
            s(b"(11 Keys:[ 54 55 ] Values:[ 69 70 ] Children:[ 0 0 0 ] Leaf:t Prev:10 Next:12)"),
            s(b"(12 Keys:[ 56 57 ] Values:[ 72 73 ] Children:[ 0 0 0 ] Leaf:t Prev:11 Next:13)"),
            s(b"(13 Keys:[ 66 67 ] Values:[ 84 86 ] Children:[ 0 0 0 ] Leaf:t Prev:12 Next:20)"),
            s(b"(20 Keys:[ 69 70 ] Values:[ 88 90 ] Children:[ 0 0 0 ] Leaf:t Prev:13 Next:15)"),
            s(b"(15 Keys:[ 76 77 ] Values:[ 97 99 ] Children:[ 0 0 0 ] Leaf:t Prev:20 Next:16)"),
            s(b"(16 Keys:[ 86 87 ] Values:[ 110 111 ] Children:[ 0 0 0 ] Leaf:t Prev:15 Next:18)"),
            s(b"(18 Keys:[ 111 120 ] Values:[ 142 154 ] Children:[ 0 0 0 ] Leaf:t Prev:16 Next:19)"),
            s(b"(19 Keys:[ 121 122 ] Values:[ 155 156 ] Children:[ 0 0 0 ] Leaf:t Prev:18 Next:0)"),
        ]);
        assert_contains_keys(&tree, vector[
            1, 2, 3,
            5, 6,
            11, 12,
            41, 42,
            54, 55,
            56, 57,
            66, 67,
            69, 70,
            76, 77,
            86, 87,
            111, 120,
            121, 122,
        ]);
        destroy_tree(tree);
    }

    #[test]
    fun test_tree_insert_split_multiple_iterations_first_elem() {
        // Tests that elements can be added to an internal node because of a leaf node split that increase the depth of
        // the tree. The internal node expands by an insertion into the first element slot.

        let tree = tree_from_strs(4, vector[
            s(b"(3 Keys:[ 50 70 100 ] Children:[ 4 5 6 7 ] Leaf:f Prev:0 Next:0)"),
            s(b"(4 Keys:[ 10 40 ] Children:[ 8 9 10 ] Leaf:f Prev:0 Next:0)"),
            s(b"(5 Keys:[ 55 60 65 68 ] Children:[ 11 12 21 13 20 ] Leaf:f Prev:0 Next:0)"),
            s(b"(6 Keys:[ 85 ] Children:[ 15 16 ] Leaf:f Prev:0 Next:0)"),
            s(b"(7 Keys:[ 120 ] Children:[ 18 19 ] Leaf:f Prev:0 Next:0)"),

            s(b"(8 Keys:[ 1 2 5 6 ] Values:[ 1 2 6 7 ] Children:[ 0 0 0 0 0 ] Leaf:t Prev:0 Next:9)"),
            s(b"(9 Keys:[ 11 12 ] Values:[ 14 15 ] Children:[ 0 0 0 ] Leaf:t Prev:8 Next:10)"),
            s(b"(10 Keys:[ 41 42 ] Values:[ 52 54 ] Children:[ 0 0 0 ] Leaf:t Prev:9 Next:11)"),
            s(b"(11 Keys:[ 51 52 53 54 ] Values:[ 65 66 68 69 ] Children:[ 0 0 0 0 0 ] Leaf:t Prev:10 Next:12)"),
            s(b"(12 Keys:[ 56 57 ] Values:[ 72 73 ] Children:[ 0 0 0 ] Leaf:t Prev:11 Next:21)"),
            s(b"(21 Keys:[ 61 62 ] Values:[ 78 79 ] Children:[ 0 0 0 ] Leaf:t Prev:12 Next:13)"),
            s(b"(13 Keys:[ 66 67 ] Values:[ 84 86 ] Children:[ 0 0 0 ] Leaf:t Prev:21 Next:20)"),
            s(b"(20 Keys:[ 69 70 ] Values:[ 88 90 ] Children:[ 0 0 0 ] Leaf:t Prev:13 Next:15)"),
            s(b"(15 Keys:[ 76 77 ] Values:[ 97 99 ] Children:[ 0 0 0 ] Leaf:t Prev:20 Next:16)"),
            s(b"(16 Keys:[ 86 87 ] Values:[ 110 111 ] Children:[ 0 0 0 ] Leaf:t Prev:15 Next:18)"),
            s(b"(18 Keys:[ 111 120 ] Values:[ 142 154 ] Children:[ 0 0 0 ] Leaf:t Prev:16 Next:19)"),
            s(b"(19 Keys:[ 121 122 ] Values:[ 155 156 ] Children:[ 0 0 0 ] Leaf:t Prev:18 Next:0)"),
        ]);
        assert_valid_tree(&tree);
        tree_insert(&mut tree, 55, 70);
        assert_valid_tree(&tree);
        assert_tree(&tree, vector[
            s(b"(3 Keys:[ 50 65 70 100 ] Children:[ 4 5 22 6 7 ] Leaf:f Prev:0 Next:0)"),
            s(b"(4 Keys:[ 10 40 ] Children:[ 8 9 10 ] Leaf:f Prev:0 Next:0)"),
            s(b"(5 Keys:[ 53 55 60 ] Children:[ 11 23 12 21 ] Leaf:f Prev:0 Next:0)"),
            s(b"(22 Keys:[ 68 ] Children:[ 13 20 ] Leaf:f Prev:0 Next:0)"),
            s(b"(6 Keys:[ 85 ] Children:[ 15 16 ] Leaf:f Prev:0 Next:0)"),
            s(b"(7 Keys:[ 120 ] Children:[ 18 19 ] Leaf:f Prev:0 Next:0)"),

            s(b"(8 Keys:[ 1 2 5 6 ] Values:[ 1 2 6 7 ] Children:[ 0 0 0 0 0 ] Leaf:t Prev:0 Next:9)"),
            s(b"(9 Keys:[ 11 12 ] Values:[ 14 15 ] Children:[ 0 0 0 ] Leaf:t Prev:8 Next:10)"),
            s(b"(10 Keys:[ 41 42 ] Values:[ 52 54 ] Children:[ 0 0 0 ] Leaf:t Prev:9 Next:11)"),
            s(b"(11 Keys:[ 51 52 53 ] Values:[ 65 66 68 ] Children:[ 0 0 0 0 ] Leaf:t Prev:10 Next:23)"),
            s(b"(23 Keys:[ 54 55 ] Values:[ 69 70 ] Children:[ 0 0 0 ] Leaf:t Prev:11 Next:12)"),
            s(b"(12 Keys:[ 56 57 ] Values:[ 72 73 ] Children:[ 0 0 0 ] Leaf:t Prev:23 Next:21)"),
            s(b"(21 Keys:[ 61 62 ] Values:[ 78 79 ] Children:[ 0 0 0 ] Leaf:t Prev:12 Next:13)"),
            s(b"(13 Keys:[ 66 67 ] Values:[ 84 86 ] Children:[ 0 0 0 ] Leaf:t Prev:21 Next:20)"),
            s(b"(20 Keys:[ 69 70 ] Values:[ 88 90 ] Children:[ 0 0 0 ] Leaf:t Prev:13 Next:15)"),
            s(b"(15 Keys:[ 76 77 ] Values:[ 97 99 ] Children:[ 0 0 0 ] Leaf:t Prev:20 Next:16)"),
            s(b"(16 Keys:[ 86 87 ] Values:[ 110 111 ] Children:[ 0 0 0 ] Leaf:t Prev:15 Next:18)"),
            s(b"(18 Keys:[ 111 120 ] Values:[ 142 154 ] Children:[ 0 0 0 ] Leaf:t Prev:16 Next:19)"),
            s(b"(19 Keys:[ 121 122 ] Values:[ 155 156 ] Children:[ 0 0 0 ] Leaf:t Prev:18 Next:0)"),
        ]);
        assert_contains_keys(&tree, vector[
            1, 2, 5, 6,
            11, 12,
            41, 42,
            51, 52, 53,
            54, 55,
            56, 57,
            61, 62,
            66, 67,
            69, 70,
            76, 77,
            86, 87,
            111, 120,
            121, 122,
        ]);
        destroy_tree(tree);
    }

    #[test]
    fun test_tree_insert_split_multiple_iterations_middle_elem() {
        // Tests that elements can be added to an internal node because of a leaf node split that increase the depth of
        // the tree. The internal node expands by an insertion into a middle element slot.

        let tree = tree_from_strs(4, vector[
            s(b"(3 Keys:[ 50 70 100 ] Children:[ 4 5 6 7 ] Leaf:f Prev:0 Next:0)"),
            s(b"(4 Keys:[ 10 40 ] Children:[ 8 9 10 ] Leaf:f Prev:0 Next:0)"),
            s(b"(5 Keys:[ 55 60 65 68 ] Children:[ 11 12 21 13 20 ] Leaf:f Prev:0 Next:0)"),
            s(b"(6 Keys:[ 85 ] Children:[ 15 16 ] Leaf:f Prev:0 Next:0)"),
            s(b"(7 Keys:[ 120 ] Children:[ 18 19 ] Leaf:f Prev:0 Next:0)"),

            s(b"(8 Keys:[ 1 2 5 6 ] Values:[ 1 2 6 7 ] Children:[ 0 0 0 0 0 ] Leaf:t Prev:0 Next:9)"),
            s(b"(9 Keys:[ 11 12 ] Values:[ 14 15 ] Children:[ 0 0 0 ] Leaf:t Prev:8 Next:10)"),
            s(b"(10 Keys:[ 41 42 ] Values:[ 52 54 ] Children:[ 0 0 0 ] Leaf:t Prev:9 Next:11)"),
            s(b"(11 Keys:[ 51 52 53 54 ] Values:[ 65 66 68 69 ] Children:[ 0 0 0 0 0 ] Leaf:t Prev:10 Next:12)"),
            s(b"(12 Keys:[ 56 57 ] Values:[ 72 73 ] Children:[ 0 0 0 ] Leaf:t Prev:11 Next:21)"),
            s(b"(21 Keys:[ 61 62 63 64 ] Values:[ 78 79 81 82 ] Children:[ 0 0 0 0 0 ] Leaf:t Prev:12 Next:13)"),
            s(b"(13 Keys:[ 66 67 ] Values:[ 84 86 ] Children:[ 0 0 0 ] Leaf:t Prev:21 Next:20)"),
            s(b"(20 Keys:[ 69 70 ] Values:[ 88 90 ] Children:[ 0 0 0 ] Leaf:t Prev:13 Next:15)"),
            s(b"(15 Keys:[ 76 77 ] Values:[ 97 99 ] Children:[ 0 0 0 ] Leaf:t Prev:20 Next:16)"),
            s(b"(16 Keys:[ 86 87 ] Values:[ 110 111 ] Children:[ 0 0 0 ] Leaf:t Prev:15 Next:18)"),
            s(b"(18 Keys:[ 111 120 ] Values:[ 142 154 ] Children:[ 0 0 0 ] Leaf:t Prev:16 Next:19)"),
            s(b"(19 Keys:[ 121 122 ] Values:[ 155 156 ] Children:[ 0 0 0 ] Leaf:t Prev:18 Next:0)"),
        ]);
        assert_valid_tree(&tree);
        tree_insert(&mut tree, 65, 83);
        assert_valid_tree(&tree);
        assert_tree(&tree, vector[
            s(b"(3 Keys:[ 50 65 70 100 ] Children:[ 4 5 22 6 7 ] Leaf:f Prev:0 Next:0)"),
            s(b"(4 Keys:[ 10 40 ] Children:[ 8 9 10 ] Leaf:f Prev:0 Next:0)"),
            s(b"(5 Keys:[ 55 60 63 ] Children:[ 11 12 21 23 ] Leaf:f Prev:0 Next:0)"),
            s(b"(22 Keys:[ 68 ] Children:[ 13 20 ] Leaf:f Prev:0 Next:0)"),
            s(b"(6 Keys:[ 85 ] Children:[ 15 16 ] Leaf:f Prev:0 Next:0)"),
            s(b"(7 Keys:[ 120 ] Children:[ 18 19 ] Leaf:f Prev:0 Next:0)"),

            s(b"(8 Keys:[ 1 2 5 6 ] Values:[ 1 2 6 7 ] Children:[ 0 0 0 0 0 ] Leaf:t Prev:0 Next:9)"),
            s(b"(9 Keys:[ 11 12 ] Values:[ 14 15 ] Children:[ 0 0 0 ] Leaf:t Prev:8 Next:10)"),
            s(b"(10 Keys:[ 41 42 ] Values:[ 52 54 ] Children:[ 0 0 0 ] Leaf:t Prev:9 Next:11)"),
            s(b"(11 Keys:[ 51 52 53 54 ] Values:[ 65 66 68 69 ] Children:[ 0 0 0 0 0 ] Leaf:t Prev:10 Next:12)"),
            s(b"(12 Keys:[ 56 57 ] Values:[ 72 73 ] Children:[ 0 0 0 ] Leaf:t Prev:11 Next:21)"),
            s(b"(21 Keys:[ 61 62 63 ] Values:[ 78 79 81 ] Children:[ 0 0 0 0 ] Leaf:t Prev:12 Next:23)"),
            s(b"(23 Keys:[ 64 65 ] Values:[ 82 83 ] Children:[ 0 0 0 ] Leaf:t Prev:21 Next:13)"),
            s(b"(13 Keys:[ 66 67 ] Values:[ 84 86 ] Children:[ 0 0 0 ] Leaf:t Prev:23 Next:20)"),
            s(b"(20 Keys:[ 69 70 ] Values:[ 88 90 ] Children:[ 0 0 0 ] Leaf:t Prev:13 Next:15)"),
            s(b"(15 Keys:[ 76 77 ] Values:[ 97 99 ] Children:[ 0 0 0 ] Leaf:t Prev:20 Next:16)"),
            s(b"(16 Keys:[ 86 87 ] Values:[ 110 111 ] Children:[ 0 0 0 ] Leaf:t Prev:15 Next:18)"),
            s(b"(18 Keys:[ 111 120 ] Values:[ 142 154 ] Children:[ 0 0 0 ] Leaf:t Prev:16 Next:19)"),
            s(b"(19 Keys:[ 121 122 ] Values:[ 155 156 ] Children:[ 0 0 0 ] Leaf:t Prev:18 Next:0)"),
        ]);
        assert_contains_keys(&tree, vector[
            1, 2, 5, 6,
            11, 12,
            41, 42,
            51, 52, 53, 54,
            56, 57,
            61, 62, 63,
            64, 65,
            66, 67,
            69, 70,
            76, 77,
            86, 87,
            111, 120,
            121, 122,
        ]);
        destroy_tree(tree);
    }

    #[test]
    fun test_tree_insert_split_multiple_iterations_end_elem() {
        // Tests that elements can be added to an internal node because of a leaf node split that increase the depth of
        // the tree. The internal node expands by an insertion into a end element slot.

        let tree = tree_from_strs(4, vector[
            s(b"(3 Keys:[ 50 75 100 ] Children:[ 4 5 6 7 ] Leaf:f Prev:0 Next:0)"),
            s(b"(4 Keys:[ 10 40 ] Children:[ 8 9 10 ] Leaf:f Prev:0 Next:0)"),
            s(b"(5 Keys:[ 55 60 65 68 ] Children:[ 11 12 21 13 20 ] Leaf:f Prev:0 Next:0)"),
            s(b"(6 Keys:[ 85 ] Children:[ 15 16 ] Leaf:f Prev:0 Next:0)"),
            s(b"(7 Keys:[ 120 ] Children:[ 18 19 ] Leaf:f Prev:0 Next:0)"),

            s(b"(8 Keys:[ 1 2 5 6 ] Values:[ 1 2 6 7 ] Children:[ 0 0 0 0 0 ] Leaf:t Prev:0 Next:9)"),
            s(b"(9 Keys:[ 11 12 ] Values:[ 14 15 ] Children:[ 0 0 0 ] Leaf:t Prev:8 Next:10)"),
            s(b"(10 Keys:[ 41 42 ] Values:[ 52 54 ] Children:[ 0 0 0 ] Leaf:t Prev:9 Next:11)"),
            s(b"(11 Keys:[ 51 52 53 54 ] Values:[ 65 66 68 69 ] Children:[ 0 0 0 0 0 ] Leaf:t Prev:10 Next:12)"),
            s(b"(12 Keys:[ 56 57 ] Values:[ 72 73 ] Children:[ 0 0 0 ] Leaf:t Prev:11 Next:21)"),
            s(b"(21 Keys:[ 61 62 63 64 ] Values:[ 78 79 81 82 ] Children:[ 0 0 0 0 0 ] Leaf:t Prev:12 Next:13)"),
            s(b"(13 Keys:[ 66 67 ] Values:[ 84 86 ] Children:[ 0 0 0 ] Leaf:t Prev:21 Next:20)"),
            s(b"(20 Keys:[ 69 70 71 72 ] Values:[ 88 90 91 92 ] Children:[ 0 0 0 0 0 ] Leaf:t Prev:13 Next:15)"),
            s(b"(15 Keys:[ 76 77 ] Values:[ 97 99 ] Children:[ 0 0 0 ] Leaf:t Prev:20 Next:16)"),
            s(b"(16 Keys:[ 86 87 ] Values:[ 110 111 ] Children:[ 0 0 0 ] Leaf:t Prev:15 Next:18)"),
            s(b"(18 Keys:[ 111 120 ] Values:[ 142 154 ] Children:[ 0 0 0 ] Leaf:t Prev:16 Next:19)"),
            s(b"(19 Keys:[ 121 122 ] Values:[ 155 156 ] Children:[ 0 0 0 ] Leaf:t Prev:18 Next:0)"),
        ]);
        assert_valid_tree(&tree);
        tree_insert(&mut tree, 73, 93);
        assert_valid_tree(&tree);
        assert_tree(&tree, vector[
            s(b"(3 Keys:[ 50 65 75 100 ] Children:[ 4 5 22 6 7 ] Leaf:f Prev:0 Next:0)"),
            s(b"(4 Keys:[ 10 40 ] Children:[ 8 9 10 ] Leaf:f Prev:0 Next:0)"),
            s(b"(5 Keys:[ 55 60 ] Children:[ 11 12 21 ] Leaf:f Prev:0 Next:0)"),
            s(b"(22 Keys:[ 68 71 ] Children:[ 13 20 23 ] Leaf:f Prev:0 Next:0)"),
            s(b"(6 Keys:[ 85 ] Children:[ 15 16 ] Leaf:f Prev:0 Next:0)"),
            s(b"(7 Keys:[ 120 ] Children:[ 18 19 ] Leaf:f Prev:0 Next:0)"),

            s(b"(8 Keys:[ 1 2 5 6 ] Values:[ 1 2 6 7 ] Children:[ 0 0 0 0 0 ] Leaf:t Prev:0 Next:9)"),
            s(b"(9 Keys:[ 11 12 ] Values:[ 14 15 ] Children:[ 0 0 0 ] Leaf:t Prev:8 Next:10)"),
            s(b"(10 Keys:[ 41 42 ] Values:[ 52 54 ] Children:[ 0 0 0 ] Leaf:t Prev:9 Next:11)"),
            s(b"(11 Keys:[ 51 52 53 54 ] Values:[ 65 66 68 69 ] Children:[ 0 0 0 0 0 ] Leaf:t Prev:10 Next:12)"),
            s(b"(12 Keys:[ 56 57 ] Values:[ 72 73 ] Children:[ 0 0 0 ] Leaf:t Prev:11 Next:21)"),
            s(b"(21 Keys:[ 61 62 63 64 ] Values:[ 78 79 81 82 ] Children:[ 0 0 0 0 0 ] Leaf:t Prev:12 Next:13)"),
            s(b"(13 Keys:[ 66 67 ] Values:[ 84 86 ] Children:[ 0 0 0 ] Leaf:t Prev:21 Next:20)"),
            s(b"(20 Keys:[ 69 70 71 ] Values:[ 88 90 91 ] Children:[ 0 0 0 0 ] Leaf:t Prev:13 Next:23)"),
            s(b"(23 Keys:[ 72 73 ] Values:[ 92 93 ] Children:[ 0 0 0 ] Leaf:t Prev:20 Next:15)"),
            s(b"(15 Keys:[ 76 77 ] Values:[ 97 99 ] Children:[ 0 0 0 ] Leaf:t Prev:23 Next:16)"),
            s(b"(16 Keys:[ 86 87 ] Values:[ 110 111 ] Children:[ 0 0 0 ] Leaf:t Prev:15 Next:18)"),
            s(b"(18 Keys:[ 111 120 ] Values:[ 142 154 ] Children:[ 0 0 0 ] Leaf:t Prev:16 Next:19)"),
            s(b"(19 Keys:[ 121 122 ] Values:[ 155 156 ] Children:[ 0 0 0 ] Leaf:t Prev:18 Next:0)"),
        ]);
        assert_contains_keys(&tree, vector[
            1, 2, 5, 6,
            11, 12,
            41, 42,
            51, 52, 53, 54,
            56, 57,
            61, 62, 63, 64,
            66, 67,
            69, 70, 71,
            72, 73,
            76, 77,
            86, 87,
            111, 120,
            121, 122,
        ]);
        destroy_tree(tree);
    }

    #[test]
    fun test_tree_delete_root_middle_no_rebalance() {
        // Tests that elements can be deleted from the middle of root without triggering a rebalance.

        let tree = tree_from_strs(8, vector[
            s(b"(3 Keys:[ 5 10 25 45 76 90 ] Values:[ 6 12 32 57 97 115 ] Children:[ 0 0 0 0 0 0 0 ] Leaf:t Prev:0 Next:0)")
        ]);
        assert_valid_tree(&tree);
        tree_delete(&mut tree, 45);
        assert_valid_tree(&tree);
        assert_tree(&tree, vector[
            s(b"(3 Keys:[ 5 10 25 76 90 ] Values:[ 6 12 32 97 115 ] Children:[ 0 0 0 0 0 0 ] Leaf:t Prev:0 Next:0)")
        ]);
        assert_contains_keys(&tree, vector[
            5, 10, 25, 76, 90
        ]);
        destroy_tree(tree);
    }

    #[test]
    fun test_tree_delete_root_start_no_rebalance() {
        // Tests that elements can be deleted from the start of root without triggering a rebalance.

        let tree = tree_from_strs(8, vector[
            s(b"(3 Keys:[ 5 10 25 45 76 90 ] Values:[ 6 12 32 57 97 115 ] Children:[ 0 0 0 0 0 0 0 ] Leaf:t Prev:0 Next:0)")
        ]);
        assert_valid_tree(&tree);
        tree_delete(&mut tree, 5);
        assert_valid_tree(&tree);
        assert_tree(&tree, vector[
            s(b"(3 Keys:[ 10 25 45 76 90 ] Values:[ 12 32 57 97 115 ] Children:[ 0 0 0 0 0 0 ] Leaf:t Prev:0 Next:0)")
        ]);
        assert_contains_keys(&tree, vector[
            10, 25, 45, 76, 90
        ]);
        destroy_tree(tree);
    }

    #[test]
    fun test_tree_delete_root_end_no_rebalance() {
        // Tests that elements can be deleted from the end of root without triggering a rebalance.

        let tree = tree_from_strs(8, vector[
            s(b"(3 Keys:[ 5 10 25 45 76 90 ] Values:[ 6 12 32 57 97 115 ] Children:[ 0 0 0 0 0 0 0 ] Leaf:t Prev:0 Next:0)")
        ]);
        assert_valid_tree(&tree);
        tree_delete(&mut tree, 90);
        assert_valid_tree(&tree);
        assert_tree(&tree, vector[
            s(b"(3 Keys:[ 5 10 25 45 76 ] Values:[ 6 12 32 57 97 ] Children:[ 0 0 0 0 0 0 ] Leaf:t Prev:0 Next:0)")
        ]);
        assert_contains_keys(&tree, vector[
            5, 10, 25, 45, 76
        ]);
        destroy_tree(tree);
    }

    #[test]
    fun test_tree_delete_root_single_elem() {
        // Tests that elements can be deleted from a root that only has a single element, resulting in an empty tree.

        let tree = tree_from_strs(8, vector[
            s(b"(3 Keys:[5] Values:[6] Children:[ 0 0 ] Leaf:t Prev:0 Next:0)")
        ]);
        assert_valid_tree(&tree);
        tree_delete(&mut tree, 5);
        assert_valid_tree(&tree);
        assert_tree(&tree, vector[]);
        assert!(tree.root == 0, 0);
        assert!(tree.min == 0, 0);
        assert!(tree.max == 0, 0);
        assert!(tree.treeSize == 0, 0);
        assert_contains_keys(&tree, vector[]);
        destroy_tree(tree);
    }

    #[test]
    fun test_tree_delete_non_root_middle_no_rebalance() {
        // Tests that elements can be deleted from the middle of a non root without triggering a rebalance.

        let tree = tree_from_strs(8, vector[
            s(b"(3 Keys:[ 5 6 7 8 9 ] Values:[ 6 7 9 10 11 ] Children:[ 0 0 0 0 0 0 ] Leaf:t Prev:0 Next:0)")
        ]);
        assert_valid_tree(&tree);
        tree_delete(&mut tree, 7);
        assert_valid_tree(&tree);
        assert_tree(&tree, vector[
            s(b"(3 Keys:[ 5 6 8 9 ] Values:[ 6 7 10 11 ] Children:[ 0 0 0 0 0 ] Leaf:t Prev:0 Next:0)"),
        ]);
        assert_contains_keys(&tree, vector[
            5, 6, 8, 9
        ]);
        destroy_tree(tree);
    }

    #[test]
    fun test_tree_delete_non_root_start_no_rebalance() {
        // Tests that elements can be deleted from the start of a non root without triggering a rebalance.

        let tree = tree_from_strs(8, vector[
            s(b"(3 Keys:[ 5 6 7 8 9 ] Values:[ 6 7 9 10 11 ] Children:[ 0 0 0 0 0 0 ] Leaf:t Prev:0 Next:0)")
        ]);
        assert_valid_tree(&tree);
        tree_delete(&mut tree, 5);
        assert_valid_tree(&tree);
        assert_tree(&tree, vector[
            s(b"(3 Keys:[ 6 7 8 9 ] Values:[ 7 9 10 11 ] Children:[ 0 0 0 0 0 ] Leaf:t Prev:0 Next:0)"),
        ]);
        assert_contains_keys(&tree, vector[
            6, 7, 8, 9
        ]);
        destroy_tree(tree);
    }

    #[test]
    fun test_tree_delete_non_root_end_no_rebalance() {
        // Tests that elements can be deleted from the end of a non root without triggering a rebalance.

        let tree = tree_from_strs(8, vector[
            s(b"(3 Keys:[ 5 6 7 8 9 ] Values:[ 6 7 9 10 11 ] Children:[ 0 0 0 0 0 0 ] Leaf:t Prev:0 Next:0)")
        ]);
        assert_valid_tree(&tree);
        tree_delete(&mut tree, 9);
        assert_valid_tree(&tree);
        assert_tree(&tree, vector[
            s(b"(3 Keys:[ 5 6 7 8 ] Values:[ 6 7 9 10 ] Children:[ 0 0 0 0 0 ] Leaf:t Prev:0 Next:0)"),
        ]);
        assert_contains_keys(&tree, vector[
            5, 6, 7, 8
        ]);
        destroy_tree(tree);
    }

    #[test]
    fun test_tree_delete_non_root_middle_left_adoption_rebalance() {
        // Tests that elements can be deleted from the middle of a non root, triggering a left adoption.

        let tree = tree_from_strs(8, vector[
            s(b"(3 Keys:[ 50 70 100 ] Children:[ 4 5 6 7 ] Leaf:f Prev:0 Next:0)"),

            s(b"(4 Keys:[ 5 6 20 28 ] Values:[ 6 7 25 36 ] Children:[ 0 0 0 0 0 ] Leaf:t Prev:0 Next:5)"),
            s(b"(5 Keys:[ 55 56 65 67 69 ] Values:[ 70 72 83 86 88 ] Children:[ 0 0 0 0 0 0 ] Leaf:t Prev:4 Next:6)"),
            s(b"(6 Keys:[ 76 77 88 89 ] Values:[ 97 99 113 114 ] Children:[ 0 0 0 0 0 ] Leaf:t Prev:5 Next:7)"),
            s(b"(7 Keys:[ 105 106 107 108 109 ] Values:[ 135 136 137 138 140 ] Children:[ 0 0 0 0 0 0 ] Leaf:t Prev:6 Next:0)"),
        ]);
        assert_valid_tree(&tree);
        tree_delete(&mut tree, 20);
        assert_valid_tree(&tree);
        assert_tree(&tree, vector[
            s(b"(3 Keys:[ 55 70 100 ] Children:[ 4 5 6 7 ] Leaf:f Prev:0 Next:0)"),

            s(b"(4 Keys:[ 5 6 28 55 ] Values:[ 6 7 36 70 ] Children:[ 0 0 0 0 0 ] Leaf:t Prev:0 Next:5)"),
            s(b"(5 Keys:[ 56 65 67 69 ] Values:[ 72 83 86 88 ] Children:[ 0 0 0 0 0 ] Leaf:t Prev:4 Next:6)"),
            s(b"(6 Keys:[ 76 77 88 89 ] Values:[ 97 99 113 114 ] Children:[ 0 0 0 0 0 ] Leaf:t Prev:5 Next:7)"),
            s(b"(7 Keys:[ 105 106 107 108 109 ] Values:[ 135 136 137 138 140 ] Children:[ 0 0 0 0 0 0 ] Leaf:t Prev:6 Next:0)"),
        ]);
        assert_contains_keys(&tree, vector[
            5, 6, 28, 55,
            56, 65, 67, 69,
            76, 77, 88, 89,
            105, 106, 107, 108, 109,
        ]);
        destroy_tree(tree);
    }

    #[test]
    fun test_tree_delete_non_root_start_left_adoption_rebalance() {
        // Tests that elements can be deleted from the start of a non root, triggering a left adoption.

        let tree = tree_from_strs(8, vector[
            s(b"(3 Keys:[ 50 70 100 ] Children:[ 4 5 6 7 ] Leaf:f Prev:0 Next:0)"),

            s(b"(4 Keys:[ 5 6 20 28 ] Values:[ 6 7 25 36 ] Children:[ 0 0 0 0 0 ] Leaf:t Prev:0 Next:5)"),
            s(b"(5 Keys:[ 55 56 65 67 69 ] Values:[ 70 72 83 86 88 ] Children:[ 0 0 0 0 0 0 ] Leaf:t Prev:4 Next:6)"),
            s(b"(6 Keys:[ 76 77 88 89 ] Values:[ 97 99 113 114 ] Children:[ 0 0 0 0 0 ] Leaf:t Prev:5 Next:7)"),
            s(b"(7 Keys:[ 105 106 107 108 109 ] Values:[ 135 136 137 138 140 ] Children:[ 0 0 0 0 0 0 ] Leaf:t Prev:6 Next:0)"),
        ]);
        assert_valid_tree(&tree);
        tree_delete(&mut tree, 5);
        assert_valid_tree(&tree);
        assert_tree(&tree, vector[
            s(b"(3 Keys:[ 55 70 100 ] Children:[ 4 5 6 7 ] Leaf:f Prev:0 Next:0)"),

            s(b"(4 Keys:[ 6 20 28 55 ] Values:[ 7 25 36 70 ] Children:[ 0 0 0 0 0 ] Leaf:t Prev:0 Next:5)"),
            s(b"(5 Keys:[ 56 65 67 69 ] Values:[ 72 83 86 88 ] Children:[ 0 0 0 0 0 ] Leaf:t Prev:4 Next:6)"),
            s(b"(6 Keys:[ 76 77 88 89 ] Values:[ 97 99 113 114 ] Children:[ 0 0 0 0 0 ] Leaf:t Prev:5 Next:7)"),
            s(b"(7 Keys:[ 105 106 107 108 109 ] Values:[ 135 136 137 138 140 ] Children:[ 0 0 0 0 0 0 ] Leaf:t Prev:6 Next:0)"),
        ]);
        assert_contains_keys(&tree, vector[
            6, 20, 28, 55,
            56, 65, 67, 69,
            76, 77, 88, 89,
            105, 106, 107, 108, 109,
        ]);
        destroy_tree(tree);
    }

    #[test]
    fun test_tree_delete_non_root_end_left_adoption_rebalance() {
        // Tests that elements can be deleted from the end of a non root, triggering a left adoption.

        let tree = tree_from_strs(8, vector[
            s(b"(3 Keys:[ 50 70 100 ] Children:[ 4 5 6 7 ] Leaf:f Prev:0 Next:0)"),

            s(b"(4 Keys:[ 5 6 20 28 ] Values:[ 6 7 25 36 ] Children:[ 0 0 0 0 0 ] Leaf:t Prev:0 Next:5)"),
            s(b"(5 Keys:[ 55 56 65 67 69 ] Values:[ 70 72 83 86 88 ] Children:[ 0 0 0 0 0 0 ] Leaf:t Prev:4 Next:6)"),
            s(b"(6 Keys:[ 76 77 88 89 ] Values:[ 97 99 113 114 ] Children:[ 0 0 0 0 0 ] Leaf:t Prev:5 Next:7)"),
            s(b"(7 Keys:[ 105 106 107 108 109 ] Values:[ 135 136 137 138 140 ] Children:[ 0 0 0 0 0 0 ] Leaf:t Prev:6 Next:0)"),
        ]);
        assert_valid_tree(&tree);
        tree_delete(&mut tree, 28);
        assert_valid_tree(&tree);
        assert_tree(&tree, vector[
            s(b"(3 Keys:[ 55 70 100 ] Children:[ 4 5 6 7 ] Leaf:f Prev:0 Next:0)"),

            s(b"(4 Keys:[ 5 6 20 55 ] Values:[ 6 7 25 70 ] Children:[ 0 0 0 0 0 ] Leaf:t Prev:0 Next:5)"),
            s(b"(5 Keys:[ 56 65 67 69 ] Values:[ 72 83 86 88 ] Children:[ 0 0 0 0 0 ] Leaf:t Prev:4 Next:6)"),
            s(b"(6 Keys:[ 76 77 88 89 ] Values:[ 97 99 113 114 ] Children:[ 0 0 0 0 0 ] Leaf:t Prev:5 Next:7)"),
            s(b"(7 Keys:[ 105 106 107 108 109 ] Values:[ 135 136 137 138 140 ] Children:[ 0 0 0 0 0 0 ] Leaf:t Prev:6 Next:0)"),
        ]);
        assert_contains_keys(&tree, vector[
            5, 6, 20, 55,
            56, 65, 67, 69,
            76, 77, 88, 89,
            105, 106, 107, 108, 109,
        ]);
        destroy_tree(tree);
    }

    #[test]
    fun test_tree_delete_non_root_middle_right_adoption_rebalance() {
        // Tests that elements can be deleted from the middle of a non root, triggering a right adoption.

        let tree = tree_from_strs(8, vector[
            s(b"(3 Keys:[ 50 70 100 ] Children:[ 4 5 6 7 ] Leaf:f Prev:0 Next:0)"),

            s(b"(4 Keys:[ 5 6 20 28 29 ] Values:[ 6 7 25 36 37 ] Children:[ 0 0 0 0 0 0 ] Leaf:t Prev:0 Next:5)"),
            s(b"(5 Keys:[ 55 56 65 67 ] Values:[ 70 72 83 86 ] Children:[ 0 0 0 0 0 ] Leaf:t Prev:4 Next:6)"),
            s(b"(6 Keys:[ 76 77 88 89 ] Values:[ 97 99 113 114 ] Children:[ 0 0 0 0 0 ] Leaf:t Prev:5 Next:7)"),
            s(b"(7 Keys:[ 105 106 107 108 109 ] Values:[ 135 136 137 138 140 ] Children:[ 0 0 0 0 0 0 ] Leaf:t Prev:6 Next:0)"),
        ]);
        assert_valid_tree(&tree);
        tree_delete(&mut tree, 65);
        assert_valid_tree(&tree);
        assert_tree(&tree, vector[
            s(b"(3 Keys:[ 28 70 100 ] Children:[ 4 5 6 7 ] Leaf:f Prev:0 Next:0)"),

            s(b"(4 Keys:[ 5 6 20 28 ] Values:[ 6 7 25 36 ] Children:[ 0 0 0 0 0 ] Leaf:t Prev:0 Next:5)"),
            s(b"(5 Keys:[ 29 55 56 67 ] Values:[ 37 70 72 86 ] Children:[ 0 0 0 0 0 ] Leaf:t Prev:4 Next:6)"),
            s(b"(6 Keys:[ 76 77 88 89 ] Values:[ 97 99 113 114 ] Children:[ 0 0 0 0 0 ] Leaf:t Prev:5 Next:7)"),
            s(b"(7 Keys:[ 105 106 107 108 109 ] Values:[ 135 136 137 138 140 ] Children:[ 0 0 0 0 0 0 ] Leaf:t Prev:6 Next:0)"),
        ]);
        assert_contains_keys(&tree, vector[
            5, 6, 20, 28,
            29, 55, 56, 67,
            76, 77, 88, 89,
            105, 106, 107, 108, 109,
        ]);
        destroy_tree(tree);
    }

    #[test]
    fun test_tree_delete_non_root_start_right_adoption_rebalance() {
        // Tests that elements can be deleted from the start of a non root, triggering a right adoption.

        let tree = tree_from_strs(8, vector[
            s(b"(3 Keys:[ 50 70 100 ] Children:[ 4 5 6 7 ] Leaf:f Prev:0 Next:0)"),

            s(b"(4 Keys:[ 5 6 20 28 29 ] Values:[ 6 7 25 36 37 ] Children:[ 0 0 0 0 0 0 ] Leaf:t Prev:0 Next:5)"),
            s(b"(5 Keys:[ 55 56 65 67 ] Values:[ 70 72 83 86 ] Children:[ 0 0 0 0 0 ] Leaf:t Prev:4 Next:6)"),
            s(b"(6 Keys:[ 76 77 88 89 ] Values:[ 97 99 113 114 ] Children:[ 0 0 0 0 0 ] Leaf:t Prev:5 Next:7)"),
            s(b"(7 Keys:[ 105 106 107 108 109 ] Values:[ 135 136 137 138 140 ] Children:[ 0 0 0 0 0 0 ] Leaf:t Prev:6 Next:0)"),
        ]);
        assert_valid_tree(&tree);
        tree_delete(&mut tree, 55);
        assert_valid_tree(&tree);
        assert_tree(&tree, vector[
            s(b"(3 Keys:[ 28 70 100 ] Children:[ 4 5 6 7 ] Leaf:f Prev:0 Next:0)"),

            s(b"(4 Keys:[ 5 6 20 28 ] Values:[ 6 7 25 36 ] Children:[ 0 0 0 0 0 ] Leaf:t Prev:0 Next:5)"),
            s(b"(5 Keys:[ 29 56 65 67 ] Values:[ 37 72 83 86 ] Children:[ 0 0 0 0 0 ] Leaf:t Prev:4 Next:6)"),
            s(b"(6 Keys:[ 76 77 88 89 ] Values:[ 97 99 113 114 ] Children:[ 0 0 0 0 0 ] Leaf:t Prev:5 Next:7)"),
            s(b"(7 Keys:[ 105 106 107 108 109 ] Values:[ 135 136 137 138 140 ] Children:[ 0 0 0 0 0 0 ] Leaf:t Prev:6 Next:0)"),
        ]);
        assert_contains_keys(&tree, vector[
            5, 6, 20, 28,
            29, 56, 65, 67,
            76, 77, 88, 89,
            105, 106, 107, 108, 109,
        ]);
        destroy_tree(tree);
    }

    #[test]
    fun test_tree_delete_non_root_end_right_adoption_rebalance() {
        // Tests that elements can be deleted from the end of a non root, triggering a right adoption.

        let tree = tree_from_strs(8, vector[
            s(b"(3 Keys:[ 50 70 100 ] Children:[ 4 5 6 7 ] Leaf:f Prev:0 Next:0)"),

            s(b"(4 Keys:[ 5 6 20 28 29 ] Values:[ 6 7 25 36 37 ] Children:[ 0 0 0 0 0 0 ] Leaf:t Prev:0 Next:5)"),
            s(b"(5 Keys:[ 55 56 65 67 ] Values:[ 70 72 83 86 ] Children:[ 0 0 0 0 0 ] Leaf:t Prev:4 Next:6)"),
            s(b"(6 Keys:[ 76 77 88 89 ] Values:[ 97 99 113 114 ] Children:[ 0 0 0 0 0 ] Leaf:t Prev:5 Next:7)"),
            s(b"(7 Keys:[ 105 106 107 108 109 ] Values:[ 135 136 137 138 140 ] Children:[ 0 0 0 0 0 0 ] Leaf:t Prev:6 Next:0)"),
        ]);
        assert_valid_tree(&tree);
        tree_delete(&mut tree, 67);
        assert_valid_tree(&tree);
        assert_tree(&tree, vector[
            s(b"(3 Keys:[ 28 70 100 ] Children:[ 4 5 6 7 ] Leaf:f Prev:0 Next:0)"),

            s(b"(4 Keys:[ 5 6 20 28 ] Values:[ 6 7 25 36 ] Children:[ 0 0 0 0 0 ] Leaf:t Prev:0 Next:5)"),
            s(b"(5 Keys:[ 29 55 56 65 ] Values:[ 37 70 72 83 ] Children:[ 0 0 0 0 0 ] Leaf:t Prev:4 Next:6)"),
            s(b"(6 Keys:[ 76 77 88 89 ] Values:[ 97 99 113 114 ] Children:[ 0 0 0 0 0 ] Leaf:t Prev:5 Next:7)"),
            s(b"(7 Keys:[ 105 106 107 108 109 ] Values:[ 135 136 137 138 140 ] Children:[ 0 0 0 0 0 0 ] Leaf:t Prev:6 Next:0)"),
        ]);
        assert_contains_keys(&tree, vector[
            5, 6, 20, 28,
            29, 55, 56, 65,
            76, 77, 88, 89,
            105, 106, 107, 108, 109,
        ]);
        destroy_tree(tree);
    }

    #[test]
    fun test_tree_delete_internal_right_adoption() {
        // Tests that a delete which triggers a right adoption in an internal node because an element from
        // the node was deleted.

        let tree = tree_from_strs(4, vector[
            s(b"(3 Keys:[ 50 70 100 ] Children:[ 4 5 6 7 ] Leaf:f Prev:0 Next:0)"),
            s(b"(4 Keys:[ 10 40 ] Children:[ 8 9 10 ] Leaf:f Prev:0 Next:0)"),
            s(b"(5 Keys:[ 55 65 ] Children:[ 11 12 13 ] Leaf:f Prev:0 Next:0)"),
            s(b"(6 Keys:[ 85 ] Children:[ 15 16 ] Leaf:f Prev:0 Next:0)"),
            s(b"(7 Keys:[ 105 110 120 ] Children:[ 20 17 18 19 ] Leaf:f Prev:0 Next:0)"),

            s(b"(8 Keys:[ 5 6 ] Values:[ 6 7 ] Children:[ 0 0 0 ] Leaf:t Prev:0 Next:9)"),
            s(b"(9 Keys:[ 11 12 ] Values:[ 14 15 ] Children:[ 0 0 0 ] Leaf:t Prev:8 Next:10)"),
            s(b"(10 Keys:[ 41 42 ] Values:[ 52 54 ] Children:[ 0 0 0 ] Leaf:t Prev:9 Next:11)"),
            s(b"(11 Keys:[ 54 55 ] Values:[ 69 70 ] Children:[ 0 0 0 ] Leaf:t Prev:10 Next:12)"),
            s(b"(12 Keys:[ 56 57 ] Values:[ 72 73 ] Children:[ 0 0 0 ] Leaf:t Prev:11 Next:13)"),
            s(b"(13 Keys:[ 66 67 ] Values:[ 84 86 ] Children:[ 0 0 0 ] Leaf:t Prev:12 Next:15)"),
            s(b"(15 Keys:[ 76 77 ] Values:[ 97 99 ] Children:[ 0 0 0 ] Leaf:t Prev:13 Next:16)"),
            s(b"(16 Keys:[ 86 87 ] Values:[ 110 111 ] Children:[ 0 0 0 ] Leaf:t Prev:15 Next:20)"),
            s(b"(20 Keys:[ 101 102 ] Values:[ 129 131 ] Children:[ 0 0 0 ] Leaf:t Prev:16 Next:17)"),
            s(b"(17 Keys:[ 109 110 ] Values:[ 140 141 ] Children:[ 0 0 0 ] Leaf:t Prev:20 Next:18)"),
            s(b"(18 Keys:[ 111 120 ] Values:[ 142 154 ] Children:[ 0 0 0 ] Leaf:t Prev:17 Next:19)"),
            s(b"(19 Keys:[ 121 122 ] Values:[ 155 156 ] Children:[ 0 0 0 ] Leaf:t Prev:18 Next:0)"),
        ]);
        assert_valid_tree(&tree);
        tree_delete(&mut tree, 87);
        assert_valid_tree(&tree);
        assert_tree(&tree, vector[
            s(b"(3 Keys:[ 50 70 105 ] Children:[ 4 5 6 7 ] Leaf:f Prev:0 Next:0)"),
            s(b"(4 Keys:[ 10 40 ] Children:[ 8 9 10 ] Leaf:f Prev:0 Next:0)"),
            s(b"(5 Keys:[ 55 65 ] Children:[ 11 12 13 ] Leaf:f Prev:0 Next:0)"),
            s(b"(6 Keys:[ 100 ] Children:[ 16 20 ] Leaf:f Prev:0 Next:0)"),
            s(b"(7 Keys:[ 110 120 ] Children:[ 17 18 19 ] Leaf:f Prev:0 Next:0)"),

            s(b"(8 Keys:[ 5 6 ] Values:[ 6 7 ] Children:[ 0 0 0 ] Leaf:t Prev:0 Next:9)"),
            s(b"(9 Keys:[ 11 12 ] Values:[ 14 15 ] Children:[ 0 0 0 ] Leaf:t Prev:8 Next:10)"),
            s(b"(10 Keys:[ 41 42 ] Values:[ 52 54 ] Children:[ 0 0 0 ] Leaf:t Prev:9 Next:11)"),
            s(b"(11 Keys:[ 54 55 ] Values:[ 69 70 ] Children:[ 0 0 0 ] Leaf:t Prev:10 Next:12)"),
            s(b"(12 Keys:[ 56 57 ] Values:[ 72 73 ] Children:[ 0 0 0 ] Leaf:t Prev:11 Next:13)"),
            s(b"(13 Keys:[ 66 67 ] Values:[ 84 86 ] Children:[ 0 0 0 ] Leaf:t Prev:12 Next:16)"),
            s(b"(16 Keys:[ 76 77 86 ] Values:[ 97 99 110 ] Children:[ 0 0 0 0 ] Leaf:t Prev:13 Next:20)"),
            s(b"(20 Keys:[ 101 102 ] Values:[ 129 131 ] Children:[ 0 0 0 ] Leaf:t Prev:16 Next:17)"),
            s(b"(17 Keys:[ 109 110 ] Values:[ 140 141 ] Children:[ 0 0 0 ] Leaf:t Prev:20 Next:18)"),
            s(b"(18 Keys:[ 111 120 ] Values:[ 142 154 ] Children:[ 0 0 0 ] Leaf:t Prev:17 Next:19)"),
            s(b"(19 Keys:[ 121 122 ] Values:[ 155 156 ] Children:[ 0 0 0 ] Leaf:t Prev:18 Next:0)"),
        ]);
        assert_contains_keys(&tree, vector[
            5, 6,
            11, 12,
            41, 42,
            54, 55,
            56, 57,
            66, 67,
            76, 77, 86,
            101, 102,
            109, 110,
            111, 120,
            121, 122,
        ]);
        destroy_tree(tree);
    }

    #[test]
    fun test_tree_delete_internal_left_adoption() {
        // Tests that a delete which triggers a right adoption in an internal node because an element from
        // the node was deleted.

        let tree = tree_from_strs(4, vector[
            s(b"(3 Keys:[ 50 70 100 ] Children:[ 4 5 6 7 ] Leaf:f Prev:0 Next:0)"),
            s(b"(4 Keys:[ 10 40 ] Children:[ 8 9 10 ] Leaf:f Prev:0 Next:0)"),
            s(b"(5 Keys:[ 55 65 68 ] Children:[ 11 12 13 20 ] Leaf:f Prev:0 Next:0)"),
            s(b"(6 Keys:[ 85 ] Children:[ 15 16 ] Leaf:f Prev:0 Next:0)"),
            s(b"(7 Keys:[ 120 ] Children:[ 18 19 ] Leaf:f Prev:0 Next:0)"),

            s(b"(8 Keys:[ 5 6 ] Values:[ 6 7 ] Children:[ 0 0 0 ] Leaf:t Prev:0 Next:9)"),
            s(b"(9 Keys:[ 11 12 ] Values:[ 14 15 ] Children:[ 0 0 0 ] Leaf:t Prev:8 Next:10)"),
            s(b"(10 Keys:[ 41 42 ] Values:[ 52 54 ] Children:[ 0 0 0 ] Leaf:t Prev:9 Next:11)"),
            s(b"(11 Keys:[ 54 55 ] Values:[ 69 70 ] Children:[ 0 0 0 ] Leaf:t Prev:10 Next:12)"),
            s(b"(12 Keys:[ 56 57 ] Values:[ 72 73 ] Children:[ 0 0 0 ] Leaf:t Prev:11 Next:13)"),
            s(b"(13 Keys:[ 66 67 ] Values:[ 84 86 ] Children:[ 0 0 0 ] Leaf:t Prev:12 Next:20)"),
            s(b"(20 Keys:[ 69 70 ] Values:[ 88 90 ] Children:[ 0 0 0 ] Leaf:t Prev:13 Next:15)"),
            s(b"(15 Keys:[ 76 77 ] Values:[ 97 99 ] Children:[ 0 0 0 ] Leaf:t Prev:20 Next:16)"),
            s(b"(16 Keys:[ 86 87 ] Values:[ 110 111 ] Children:[ 0 0 0 ] Leaf:t Prev:15 Next:18)"),
            s(b"(18 Keys:[ 111 120 ] Values:[ 142 154 ] Children:[ 0 0 0 ] Leaf:t Prev:16 Next:19)"),
            s(b"(19 Keys:[ 121 122 ] Values:[ 155 156 ] Children:[ 0 0 0 ] Leaf:t Prev:18 Next:0)"),
        ]);
        assert_valid_tree(&tree);
        tree_delete(&mut tree, 87);
        assert_valid_tree(&tree);
        assert_tree(&tree, vector[
            s(b"(3 Keys:[ 50 68 100 ] Children:[ 4 5 6 7 ] Leaf:f Prev:0 Next:0)"),
            s(b"(4 Keys:[ 10 40 ] Children:[ 8 9 10 ] Leaf:f Prev:0 Next:0)"),
            s(b"(5 Keys:[ 55 65 ] Children:[ 11 12 13 ] Leaf:f Prev:0 Next:0)"),
            s(b"(6 Keys:[ 70 ] Children:[ 20 16 ] Leaf:f Prev:0 Next:0)"),
            s(b"(7 Keys:[ 120 ] Children:[ 18 19 ] Leaf:f Prev:0 Next:0)"),

            s(b"(8 Keys:[ 5 6 ] Values:[ 6 7 ] Children:[ 0 0 0 ] Leaf:t Prev:0 Next:9)"),
            s(b"(9 Keys:[ 11 12 ] Values:[ 14 15 ] Children:[ 0 0 0 ] Leaf:t Prev:8 Next:10)"),
            s(b"(10 Keys:[ 41 42 ] Values:[ 52 54 ] Children:[ 0 0 0 ] Leaf:t Prev:9 Next:11)"),
            s(b"(11 Keys:[ 54 55 ] Values:[ 69 70 ] Children:[ 0 0 0 ] Leaf:t Prev:10 Next:12)"),
            s(b"(12 Keys:[ 56 57 ] Values:[ 72 73 ] Children:[ 0 0 0 ] Leaf:t Prev:11 Next:13)"),
            s(b"(13 Keys:[ 66 67 ] Values:[ 84 86 ] Children:[ 0 0 0 ] Leaf:t Prev:12 Next:20)"),
            s(b"(20 Keys:[ 69 70 ] Values:[ 88 90 ] Children:[ 0 0 0 ] Leaf:t Prev:13 Next:16)"),
            s(b"(16 Keys:[ 76 77 86 ] Values:[ 97 99 110 ] Children:[ 0 0 0 0 ] Leaf:t Prev:20 Next:18)"),
            s(b"(18 Keys:[ 111 120 ] Values:[ 142 154 ] Children:[ 0 0 0 ] Leaf:t Prev:16 Next:19)"),
            s(b"(19 Keys:[ 121 122 ] Values:[ 155 156 ] Children:[ 0 0 0 ] Leaf:t Prev:18 Next:0)"),
        ]);
        assert_contains_keys(&tree, vector[
            5, 6,
            11, 12,
            41, 42,
            54, 55,
            56, 57,
            66, 67,
            69, 70,
            76, 77, 86,
            111, 120,
            121, 122,
        ]);
        destroy_tree(tree);
    }

    #[test]
    fun test_tree_delete_non_root_start_right_merge_rebalance_no_propogate() {
        // Tests that elements can be deleted from the start of a non root, triggering a right merge.
        // The merge shouldn't propogate beyond the first level.

        let tree = tree_from_strs(4, vector[
            s(b"(3 Keys:[ 50 70 100 ] Children:[ 4 5 6 7 ] Leaf:f Prev:0 Next:0)"),
            s(b"(4 Keys:[ 10 40 ] Children:[ 8 9 10 ] Leaf:f Prev:0 Next:0)"),
            s(b"(5 Keys:[ 55 65 ] Children:[ 11 12 13 ] Leaf:f Prev:0 Next:0)"),
            s(b"(6 Keys:[ 85 ] Children:[ 14 15 ] Leaf:f Prev:0 Next:0)"),
            s(b"(7 Keys:[ 105 110 120 ] Children:[ 16 17 18 19 ] Leaf:f Prev:0 Next:0)"),

            s(b"(8 Keys:[ 5 6 ] Values:[ 6 7 ] Children:[ 0 0 0 ] Leaf:t Prev:0 Next:9)"),
            s(b"(9 Keys:[ 11 12 ] Values:[ 14 15 ] Children:[ 0 0 0 ] Leaf:t Prev:8 Next:10)"),
            s(b"(10 Keys:[ 41 42 ] Values:[ 52 54 ] Children:[ 0 0 0 ] Leaf:t Prev:9 Next:11)"),
            s(b"(11 Keys:[ 54 55 ] Values:[ 69 70 ] Children:[ 0 0 0 ] Leaf:t Prev:10 Next:12)"),
            s(b"(12 Keys:[ 56 57 ] Values:[ 72 73 ] Children:[ 0 0 0 ] Leaf:t Prev:11 Next:13)"),
            s(b"(13 Keys:[ 66 67 ] Values:[ 84 86 ] Children:[ 0 0 0 ] Leaf:t Prev:12 Next:14)"),
            s(b"(14 Keys:[ 76 77 ] Values:[ 97 99 ] Children:[ 0 0 0 ] Leaf:t Prev:13 Next:15)"),
            s(b"(15 Keys:[ 86 87 ] Values:[ 110 111 ] Children:[ 0 0 0 ] Leaf:t Prev:14 Next:16)"),
            s(b"(16 Keys:[ 101 102 ] Values:[ 129 131 ] Children:[ 0 0 0 ] Leaf:t Prev:15 Next:17)"),
            s(b"(17 Keys:[ 109 110 ] Values:[ 140 141 ] Children:[ 0 0 0 ] Leaf:t Prev:16 Next:18)"),
            s(b"(18 Keys:[ 111 120 ] Values:[ 142 154 ] Children:[ 0 0 0 ] Leaf:t Prev:17 Next:19)"),
            s(b"(19 Keys:[ 121 122 ] Values:[ 155 156 ] Children:[ 0 0 0 ] Leaf:t Prev:18 Next:0)"),
        ]);
        assert_valid_tree(&tree);
        tree_delete(&mut tree, 101);
        assert_valid_tree(&tree);
        assert_tree(&tree, vector[
            s(b"(3 Keys:[ 50 70 100 ] Children:[ 4 5 6 7 ] Leaf:f Prev:0 Next:0)"),
            s(b"(4 Keys:[ 10 40 ] Children:[ 8 9 10 ] Leaf:f Prev:0 Next:0)"),
            s(b"(5 Keys:[ 55 65 ] Children:[ 11 12 13 ] Leaf:f Prev:0 Next:0)"),
            s(b"(6 Keys:[ 85 ] Children:[ 14 15 ] Leaf:f Prev:0 Next:0)"),
            s(b"(7 Keys:[ 110 120 ] Children:[ 16 18 19 ] Leaf:f Prev:0 Next:0)"),

            s(b"(8 Keys:[ 5 6 ] Values:[ 6 7 ] Children:[ 0 0 0 ] Leaf:t Prev:0 Next:9)"),
            s(b"(9 Keys:[ 11 12 ] Values:[ 14 15 ] Children:[ 0 0 0 ] Leaf:t Prev:8 Next:10)"),
            s(b"(10 Keys:[ 41 42 ] Values:[ 52 54 ] Children:[ 0 0 0 ] Leaf:t Prev:9 Next:11)"),
            s(b"(11 Keys:[ 54 55 ] Values:[ 69 70 ] Children:[ 0 0 0 ] Leaf:t Prev:10 Next:12)"),
            s(b"(12 Keys:[ 56 57 ] Values:[ 72 73 ] Children:[ 0 0 0 ] Leaf:t Prev:11 Next:13)"),
            s(b"(13 Keys:[ 66 67 ] Values:[ 84 86 ] Children:[ 0 0 0 ] Leaf:t Prev:12 Next:14)"),
            s(b"(14 Keys:[ 76 77 ] Values:[ 97 99 ] Children:[ 0 0 0 ] Leaf:t Prev:13 Next:15)"),
            s(b"(15 Keys:[ 86 87 ] Values:[ 110 111 ] Children:[ 0 0 0 ] Leaf:t Prev:14 Next:16)"),
            s(b"(16 Keys:[ 102 109 110 ] Values:[ 131 140 141 ] Children:[ 0 0 0 0 ] Leaf:t Prev:15 Next:18)"),
            s(b"(18 Keys:[ 111 120 ] Values:[ 142 154 ] Children:[ 0 0 0 ] Leaf:t Prev:16 Next:19)"),
            s(b"(19 Keys:[ 121 122 ] Values:[ 155 156 ] Children:[ 0 0 0 ] Leaf:t Prev:18 Next:0)"),
        ]);
        assert_contains_keys(&tree, vector[
            5, 6,
            11, 12,
            41, 42,
            54, 55,
            56, 57,
            66, 67,
            76, 77,
            86, 87,
            102, 109, 110,
            111, 120,
            121, 122,
        ]);
        destroy_tree(tree);
    }

    #[test]
    fun test_tree_delete_non_root_start_left_merge_rebalance_no_propogate() {
        // Tests that elements can be deleted from the start of a non root, triggering a left merge.
        // The merge shouldn't propogate beyond the first level.

        let tree = tree_from_strs(4, vector[
            s(b"(3 Keys:[ 50 70 100 ] Children:[ 4 5 6 7 ] Leaf:f Prev:0 Next:0)"),
            s(b"(4 Keys:[ 10 40 ] Children:[ 8 9 10 ] Leaf:f Prev:0 Next:0)"),
            s(b"(5 Keys:[ 55 65 ] Children:[ 11 12 13 ] Leaf:f Prev:0 Next:0)"),
            s(b"(6 Keys:[ 85 ] Children:[ 14 15 ] Leaf:f Prev:0 Next:0)"),
            s(b"(7 Keys:[ 105 110 120 ] Children:[ 16 17 18 19 ] Leaf:f Prev:0 Next:0)"),

            s(b"(8 Keys:[ 5 6 ] Values:[ 6 7 ] Children:[ 0 0 0 ] Leaf:t Prev:0 Next:9)"),
            s(b"(9 Keys:[ 11 12 ] Values:[ 14 15 ] Children:[ 0 0 0 ] Leaf:t Prev:8 Next:10)"),
            s(b"(10 Keys:[ 41 42 ] Values:[ 52 54 ] Children:[ 0 0 0 ] Leaf:t Prev:9 Next:11)"),
            s(b"(11 Keys:[ 54 55 ] Values:[ 69 70 ] Children:[ 0 0 0 ] Leaf:t Prev:10 Next:12)"),
            s(b"(12 Keys:[ 56 57 ] Values:[ 72 73 ] Children:[ 0 0 0 ] Leaf:t Prev:11 Next:13)"),
            s(b"(13 Keys:[ 66 67 ] Values:[ 84 86 ] Children:[ 0 0 0 ] Leaf:t Prev:12 Next:14)"),
            s(b"(14 Keys:[ 76 77 ] Values:[ 97 99 ] Children:[ 0 0 0 ] Leaf:t Prev:13 Next:15)"),
            s(b"(15 Keys:[ 86 87 ] Values:[ 110 111 ] Children:[ 0 0 0 ] Leaf:t Prev:14 Next:16)"),
            s(b"(16 Keys:[ 101 102 ] Values:[ 129 131 ] Children:[ 0 0 0 ] Leaf:t Prev:15 Next:17)"),
            s(b"(17 Keys:[ 109 110 ] Values:[ 140 141 ] Children:[ 0 0 0 ] Leaf:t Prev:16 Next:18)"),
            s(b"(18 Keys:[ 111 120 ] Values:[ 142 154 ] Children:[ 0 0 0 ] Leaf:t Prev:17 Next:19)"),
            s(b"(19 Keys:[ 121 122 ] Values:[ 155 156 ] Children:[ 0 0 0 ] Leaf:t Prev:18 Next:0)"),
        ]);
        assert_valid_tree(&tree);
        tree_delete(&mut tree, 121);
        assert_valid_tree(&tree);
        assert_tree(&tree, vector[
            s(b"(3 Keys:[ 50 70 100 ] Children:[ 4 5 6 7 ] Leaf:f Prev:0 Next:0)"),
            s(b"(4 Keys:[ 10 40 ] Children:[ 8 9 10 ] Leaf:f Prev:0 Next:0)"),
            s(b"(5 Keys:[ 55 65 ] Children:[ 11 12 13 ] Leaf:f Prev:0 Next:0)"),
            s(b"(6 Keys:[ 85 ] Children:[ 14 15 ] Leaf:f Prev:0 Next:0)"),
            s(b"(7 Keys:[ 105 110 ] Children:[ 16 17 19 ] Leaf:f Prev:0 Next:0)"),

            s(b"(8 Keys:[ 5 6 ] Values:[ 6 7 ] Children:[ 0 0 0 ] Leaf:t Prev:0 Next:9)"),
            s(b"(9 Keys:[ 11 12 ] Values:[ 14 15 ] Children:[ 0 0 0 ] Leaf:t Prev:8 Next:10)"),
            s(b"(10 Keys:[ 41 42 ] Values:[ 52 54 ] Children:[ 0 0 0 ] Leaf:t Prev:9 Next:11)"),
            s(b"(11 Keys:[ 54 55 ] Values:[ 69 70 ] Children:[ 0 0 0 ] Leaf:t Prev:10 Next:12)"),
            s(b"(12 Keys:[ 56 57 ] Values:[ 72 73 ] Children:[ 0 0 0 ] Leaf:t Prev:11 Next:13)"),
            s(b"(13 Keys:[ 66 67 ] Values:[ 84 86 ] Children:[ 0 0 0 ] Leaf:t Prev:12 Next:14)"),
            s(b"(14 Keys:[ 76 77 ] Values:[ 97 99 ] Children:[ 0 0 0 ] Leaf:t Prev:13 Next:15)"),
            s(b"(15 Keys:[ 86 87 ] Values:[ 110 111 ] Children:[ 0 0 0 ] Leaf:t Prev:14 Next:16)"),
            s(b"(16 Keys:[ 101 102 ] Values:[ 129 131 ] Children:[ 0 0 0 ] Leaf:t Prev:15 Next:17)"),
            s(b"(17 Keys:[ 109 110 ] Values:[ 140 141 ] Children:[ 0 0 0 ] Leaf:t Prev:16 Next:19)"),
            s(b"(19 Keys:[ 111 120 122 ] Values:[ 142 154 156 ] Children:[ 0 0 0 0 ] Leaf:t Prev:17 Next:0)"),
        ]);
        assert_contains_keys(&tree, vector[
            5, 6,
            11, 12,
            41, 42,
            54, 55,
            56, 57,
            66, 67,
            76, 77,
            86, 87,
            101, 102,
            109, 110,
            111, 120, 122,
        ]);
        destroy_tree(tree);
    }

    #[test]
    fun test_tree_delete_non_root_end_right_merge_rebalance_no_propogate() {
        // Tests that elements can be deleted from the end of a non root, triggering a right merge.
        // The merge shouldn't propogate beyond the first level.

        let tree = tree_from_strs(4, vector[
            s(b"(3 Keys:[ 50 70 100 ] Children:[ 4 5 6 7 ] Leaf:f Prev:0 Next:0)"),
            s(b"(4 Keys:[ 10 40 ] Children:[ 8 9 10 ] Leaf:f Prev:0 Next:0)"),
            s(b"(5 Keys:[ 55 65 ] Children:[ 11 12 13 ] Leaf:f Prev:0 Next:0)"),
            s(b"(6 Keys:[ 85 ] Children:[ 14 15 ] Leaf:f Prev:0 Next:0)"),
            s(b"(7 Keys:[ 105 110 120 ] Children:[ 16 17 18 19 ] Leaf:f Prev:0 Next:0)"),

            s(b"(8 Keys:[ 5 6 ] Values:[ 6 7 ] Children:[ 0 0 0 ] Leaf:t Prev:0 Next:9)"),
            s(b"(9 Keys:[ 11 12 ] Values:[ 14 15 ] Children:[ 0 0 0 ] Leaf:t Prev:8 Next:10)"),
            s(b"(10 Keys:[ 41 42 ] Values:[ 52 54 ] Children:[ 0 0 0 ] Leaf:t Prev:9 Next:11)"),
            s(b"(11 Keys:[ 54 55 ] Values:[ 69 70 ] Children:[ 0 0 0 ] Leaf:t Prev:10 Next:12)"),
            s(b"(12 Keys:[ 56 57 ] Values:[ 72 73 ] Children:[ 0 0 0 ] Leaf:t Prev:11 Next:13)"),
            s(b"(13 Keys:[ 66 67 ] Values:[ 84 86 ] Children:[ 0 0 0 ] Leaf:t Prev:12 Next:14)"),
            s(b"(14 Keys:[ 76 77 ] Values:[ 97 99 ] Children:[ 0 0 0 ] Leaf:t Prev:13 Next:15)"),
            s(b"(15 Keys:[ 86 87 ] Values:[ 110 111 ] Children:[ 0 0 0 ] Leaf:t Prev:14 Next:16)"),
            s(b"(16 Keys:[ 101 102 ] Values:[ 129 131 ] Children:[ 0 0 0 ] Leaf:t Prev:15 Next:17)"),
            s(b"(17 Keys:[ 109 110 ] Values:[ 140 141 ] Children:[ 0 0 0 ] Leaf:t Prev:16 Next:18)"),
            s(b"(18 Keys:[ 111 120 ] Values:[ 142 154 ] Children:[ 0 0 0 ] Leaf:t Prev:17 Next:19)"),
            s(b"(19 Keys:[ 121 122 ] Values:[ 155 156 ] Children:[ 0 0 0 ] Leaf:t Prev:18 Next:0)"),
        ]);
        assert_valid_tree(&tree);
        tree_delete(&mut tree, 102);
        assert_valid_tree(&tree);
        assert_tree(&tree, vector[
            s(b"(3 Keys:[ 50 70 100 ] Children:[ 4 5 6 7 ] Leaf:f Prev:0 Next:0)"),
            s(b"(4 Keys:[ 10 40 ] Children:[ 8 9 10 ] Leaf:f Prev:0 Next:0)"),
            s(b"(5 Keys:[ 55 65 ] Children:[ 11 12 13 ] Leaf:f Prev:0 Next:0)"),
            s(b"(6 Keys:[ 85 ] Children:[ 14 15 ] Leaf:f Prev:0 Next:0)"),
            s(b"(7 Keys:[ 110 120 ] Children:[ 16 18 19 ] Leaf:f Prev:0 Next:0)"),

            s(b"(8 Keys:[ 5 6 ] Values:[ 6 7 ] Children:[ 0 0 0 ] Leaf:t Prev:0 Next:9)"),
            s(b"(9 Keys:[ 11 12 ] Values:[ 14 15 ] Children:[ 0 0 0 ] Leaf:t Prev:8 Next:10)"),
            s(b"(10 Keys:[ 41 42 ] Values:[ 52 54 ] Children:[ 0 0 0 ] Leaf:t Prev:9 Next:11)"),
            s(b"(11 Keys:[ 54 55 ] Values:[ 69 70 ] Children:[ 0 0 0 ] Leaf:t Prev:10 Next:12)"),
            s(b"(12 Keys:[ 56 57 ] Values:[ 72 73 ] Children:[ 0 0 0 ] Leaf:t Prev:11 Next:13)"),
            s(b"(13 Keys:[ 66 67 ] Values:[ 84 86 ] Children:[ 0 0 0 ] Leaf:t Prev:12 Next:14)"),
            s(b"(14 Keys:[ 76 77 ] Values:[ 97 99 ] Children:[ 0 0 0 ] Leaf:t Prev:13 Next:15)"),
            s(b"(15 Keys:[ 86 87 ] Values:[ 110 111 ] Children:[ 0 0 0 ] Leaf:t Prev:14 Next:16)"),
            s(b"(16 Keys:[ 101 109 110 ] Values:[ 129 140 141 ] Children:[ 0 0 0 0 ] Leaf:t Prev:15 Next:18)"),
            s(b"(18 Keys:[ 111 120 ] Values:[ 142 154 ] Children:[ 0 0 0 ] Leaf:t Prev:16 Next:19)"),
            s(b"(19 Keys:[ 121 122 ] Values:[ 155 156 ] Children:[ 0 0 0 ] Leaf:t Prev:18 Next:0)"),
        ]);
        assert_contains_keys(&tree, vector[
            5, 6,
            11, 12,
            41, 42,
            54, 55,
            56, 57,
            66, 67,
            76, 77,
            86, 87,
            101, 109, 110,
            111, 120,
            121, 122,
        ]);
        destroy_tree(tree);

    }

    #[test]
    fun test_tree_delete_non_root_end_left_merge_rebalance_no_propogate() {
        // Tests that elements can be deleted from the end of a non root, triggering a left merge.
        // The merge shouldn't propogate beyond the first level.

        let tree = tree_from_strs(4, vector[
            s(b"(3 Keys:[ 50 70 100 ] Children:[ 4 5 6 7 ] Leaf:f Prev:0 Next:0)"),
            s(b"(4 Keys:[ 10 40 ] Children:[ 8 9 10 ] Leaf:f Prev:0 Next:0)"),
            s(b"(5 Keys:[ 55 65 ] Children:[ 11 12 13 ] Leaf:f Prev:0 Next:0)"),
            s(b"(6 Keys:[ 85 ] Children:[ 14 15 ] Leaf:f Prev:0 Next:0)"),
            s(b"(7 Keys:[ 105 110 120 ] Children:[ 16 17 18 19 ] Leaf:f Prev:0 Next:0)"),

            s(b"(8 Keys:[ 5 6 ] Values:[ 6 7 ] Children:[ 0 0 0 ] Leaf:t Prev:0 Next:9)"),
            s(b"(9 Keys:[ 11 12 ] Values:[ 14 15 ] Children:[ 0 0 0 ] Leaf:t Prev:8 Next:10)"),
            s(b"(10 Keys:[ 41 42 ] Values:[ 52 54 ] Children:[ 0 0 0 ] Leaf:t Prev:9 Next:11)"),
            s(b"(11 Keys:[ 54 55 ] Values:[ 69 70 ] Children:[ 0 0 0 ] Leaf:t Prev:10 Next:12)"),
            s(b"(12 Keys:[ 56 57 ] Values:[ 72 73 ] Children:[ 0 0 0 ] Leaf:t Prev:11 Next:13)"),
            s(b"(13 Keys:[ 66 67 ] Values:[ 84 86 ] Children:[ 0 0 0 ] Leaf:t Prev:12 Next:14)"),
            s(b"(14 Keys:[ 76 77 ] Values:[ 97 99 ] Children:[ 0 0 0 ] Leaf:t Prev:13 Next:15)"),
            s(b"(15 Keys:[ 86 87 ] Values:[ 110 111 ] Children:[ 0 0 0 ] Leaf:t Prev:14 Next:16)"),
            s(b"(16 Keys:[ 101 102 ] Values:[ 129 131 ] Children:[ 0 0 0 ] Leaf:t Prev:15 Next:17)"),
            s(b"(17 Keys:[ 109 110 ] Values:[ 140 141 ] Children:[ 0 0 0 ] Leaf:t Prev:16 Next:18)"),
            s(b"(18 Keys:[ 111 120 ] Values:[ 142 154 ] Children:[ 0 0 0 ] Leaf:t Prev:17 Next:19)"),
            s(b"(19 Keys:[ 121 122 ] Values:[ 155 156 ] Children:[ 0 0 0 ] Leaf:t Prev:18 Next:0)"),
        ]);
        assert_valid_tree(&tree);
        tree_delete(&mut tree, 122);
        assert_valid_tree(&tree);
        assert_tree(&tree, vector[
            s(b"(3 Keys:[ 50 70 100 ] Children:[ 4 5 6 7 ] Leaf:f Prev:0 Next:0)"),
            s(b"(4 Keys:[ 10 40 ] Children:[ 8 9 10 ] Leaf:f Prev:0 Next:0)"),
            s(b"(5 Keys:[ 55 65 ] Children:[ 11 12 13 ] Leaf:f Prev:0 Next:0)"),
            s(b"(6 Keys:[ 85 ] Children:[ 14 15 ] Leaf:f Prev:0 Next:0)"),
            s(b"(7 Keys:[ 105 110 ] Children:[ 16 17 19 ] Leaf:f Prev:0 Next:0)"),

            s(b"(8 Keys:[ 5 6 ] Values:[ 6 7 ] Children:[ 0 0 0 ] Leaf:t Prev:0 Next:9)"),
            s(b"(9 Keys:[ 11 12 ] Values:[ 14 15 ] Children:[ 0 0 0 ] Leaf:t Prev:8 Next:10)"),
            s(b"(10 Keys:[ 41 42 ] Values:[ 52 54 ] Children:[ 0 0 0 ] Leaf:t Prev:9 Next:11)"),
            s(b"(11 Keys:[ 54 55 ] Values:[ 69 70 ] Children:[ 0 0 0 ] Leaf:t Prev:10 Next:12)"),
            s(b"(12 Keys:[ 56 57 ] Values:[ 72 73 ] Children:[ 0 0 0 ] Leaf:t Prev:11 Next:13)"),
            s(b"(13 Keys:[ 66 67 ] Values:[ 84 86 ] Children:[ 0 0 0 ] Leaf:t Prev:12 Next:14)"),
            s(b"(14 Keys:[ 76 77 ] Values:[ 97 99 ] Children:[ 0 0 0 ] Leaf:t Prev:13 Next:15)"),
            s(b"(15 Keys:[ 86 87 ] Values:[ 110 111 ] Children:[ 0 0 0 ] Leaf:t Prev:14 Next:16)"),
            s(b"(16 Keys:[ 101 102 ] Values:[ 129 131 ] Children:[ 0 0 0 ] Leaf:t Prev:15 Next:17)"),
            s(b"(17 Keys:[ 109 110 ] Values:[ 140 141 ] Children:[ 0 0 0 ] Leaf:t Prev:16 Next:19)"),
            s(b"(19 Keys:[ 111 120 121 ] Values:[ 142 154 155 ] Children:[ 0 0 0 0 ] Leaf:t Prev:17 Next:0)"),
        ]);
        assert_contains_keys(&tree, vector[
            5, 6,
            11, 12,
            41, 42,
            54, 55,
            56, 57,
            66, 67,
            76, 77,
            86, 87,
            101, 102,
            109, 110,
            111, 120, 121,
        ]);
        destroy_tree(tree);
    }

    #[test]
    fun test_tree_delete_non_root_start_right_merge_rebalance_single_level_propogate() {
        // Tests that elements can be deleted from the start of a non root, triggering a right merge.
        // The merge should propogate only one additional level.

        let tree = tree_from_strs(4, vector[
            s(b"(3 Keys:[ 50 70 100 ] Children:[ 4 5 6 7 ] Leaf:f Prev:0 Next:0)"),
            s(b"(4 Keys:[ 10 40 ] Children:[ 8 9 10 ] Leaf:f Prev:0 Next:0)"),
            s(b"(5 Keys:[ 55 ] Children:[ 11 12 ] Leaf:f Prev:0 Next:0)"),
            s(b"(6 Keys:[ 85 ] Children:[ 14 15 ] Leaf:f Prev:0 Next:0)"),
            s(b"(7 Keys:[ 105 ] Children:[ 16 17 ] Leaf:f Prev:0 Next:0)"),

            s(b"(8 Keys:[ 5 6 ] Values:[ 6 7 ] Children:[ 0 0 0 ] Leaf:t Prev:0 Next:9)"),
            s(b"(9 Keys:[ 11 12 ] Values:[ 14 15 ] Children:[ 0 0 0 ] Leaf:t Prev:8 Next:10)"),
            s(b"(10 Keys:[ 41 42 ] Values:[ 52 54 ] Children:[ 0 0 0 ] Leaf:t Prev:9 Next:11)"),
            s(b"(11 Keys:[ 54 55 ] Values:[ 69 70 ] Children:[ 0 0 0 ] Leaf:t Prev:10 Next:12)"),
            s(b"(12 Keys:[ 56 57 ] Values:[ 72 73 ] Children:[ 0 0 0 ] Leaf:t Prev:11 Next:14)"),
            s(b"(14 Keys:[ 76 77 ] Values:[ 97 99 ] Children:[ 0 0 0 ] Leaf:t Prev:12 Next:15)"),
            s(b"(15 Keys:[ 86 87 ] Values:[ 110 111 ] Children:[ 0 0 0 ] Leaf:t Prev:14 Next:16)"),
            s(b"(16 Keys:[ 101 102 ] Values:[ 129 131 ] Children:[ 0 0 0 ] Leaf:t Prev:15 Next:17)"),
            s(b"(17 Keys:[ 109 110 ] Values:[ 140 141 ] Children:[ 0 0 0 ] Leaf:t Prev:16 Next:0)"),
        ]);
        assert_valid_tree(&tree);
        tree_delete(&mut tree, 76);
        assert_valid_tree(&tree);
        assert_tree(&tree, vector[
            s(b"(3 Keys:[ 50 70 ] Children:[ 4 5 6 ] Leaf:f Prev:0 Next:0)"),
            s(b"(4 Keys:[ 10 40 ] Children:[ 8 9 10 ] Leaf:f Prev:0 Next:0)"),
            s(b"(5 Keys:[ 55 ] Children:[ 11 12 ] Leaf:f Prev:0 Next:0)"),
            s(b"(6 Keys:[ 100 105 ] Children:[ 14 16 17 ] Leaf:f Prev:0 Next:0)"),

            s(b"(8 Keys:[ 5 6 ] Values:[ 6 7 ] Children:[ 0 0 0 ] Leaf:t Prev:0 Next:9)"),
            s(b"(9 Keys:[ 11 12 ] Values:[ 14 15 ] Children:[ 0 0 0 ] Leaf:t Prev:8 Next:10)"),
            s(b"(10 Keys:[ 41 42 ] Values:[ 52 54 ] Children:[ 0 0 0 ] Leaf:t Prev:9 Next:11)"),
            s(b"(11 Keys:[ 54 55 ] Values:[ 69 70 ] Children:[ 0 0 0 ] Leaf:t Prev:10 Next:12)"),
            s(b"(12 Keys:[ 56 57 ] Values:[ 72 73 ] Children:[ 0 0 0 ] Leaf:t Prev:11 Next:14)"),
            s(b"(14 Keys:[ 77 86 87 ] Values:[ 99 110 111 ] Children:[ 0 0 0 0 ] Leaf:t Prev:12 Next:16)"),
            s(b"(16 Keys:[ 101 102 ] Values:[ 129 131 ] Children:[ 0 0 0 ] Leaf:t Prev:14 Next:17)"),
            s(b"(17 Keys:[ 109 110 ] Values:[ 140 141 ] Children:[ 0 0 0 ] Leaf:t Prev:16 Next:0)"),
        ]);
        assert_contains_keys(&tree, vector[
            5, 6,
            11, 12,
            41, 42,
            54, 55,
            56, 57,
            77, 86, 87,
            101, 102,
            109, 110,
        ]);
        destroy_tree(tree);
    }

    #[test]
    fun test_tree_delete_non_root_start_left_merge_rebalance_single_level_propogate() {
        // Tests that elements can be deleted from the start of a non root, triggering a left merge.
        // The merge should propogate only one additional level.

        let tree = tree_from_strs(4, vector[
            s(b"(3 Keys:[ 50 70 100 ] Children:[ 4 5 6 7 ] Leaf:f Prev:0 Next:0)"),
            s(b"(4 Keys:[ 10 40 ] Children:[ 8 9 10 ] Leaf:f Prev:0 Next:0)"),
            s(b"(5 Keys:[ 55 ] Children:[ 11 12 ] Leaf:f Prev:0 Next:0)"),
            s(b"(6 Keys:[ 85 ] Children:[ 14 15 ] Leaf:f Prev:0 Next:0)"),
            s(b"(7 Keys:[ 105 ] Children:[ 16 17 ] Leaf:f Prev:0 Next:0)"),

            s(b"(8 Keys:[ 5 6 ] Values:[ 6 7 ] Children:[ 0 0 0 ] Leaf:t Prev:0 Next:9)"),
            s(b"(9 Keys:[ 11 12 ] Values:[ 14 15 ] Children:[ 0 0 0 ] Leaf:t Prev:8 Next:10)"),
            s(b"(10 Keys:[ 41 42 ] Values:[ 52 54 ] Children:[ 0 0 0 ] Leaf:t Prev:9 Next:11)"),
            s(b"(11 Keys:[ 54 55 ] Values:[ 69 70 ] Children:[ 0 0 0 ] Leaf:t Prev:10 Next:12)"),
            s(b"(12 Keys:[ 56 57 ] Values:[ 72 73 ] Children:[ 0 0 0 ] Leaf:t Prev:11 Next:14)"),
            s(b"(14 Keys:[ 76 77 ] Values:[ 97 99 ] Children:[ 0 0 0 ] Leaf:t Prev:12 Next:15)"),
            s(b"(15 Keys:[ 86 87 ] Values:[ 110 111 ] Children:[ 0 0 0 ] Leaf:t Prev:14 Next:16)"),
            s(b"(16 Keys:[ 101 102 ] Values:[ 129 131 ] Children:[ 0 0 0 ] Leaf:t Prev:15 Next:17)"),
            s(b"(17 Keys:[ 109 110 ] Values:[ 140 141 ] Children:[ 0 0 0 ] Leaf:t Prev:16 Next:0)"),
        ]);
        assert_valid_tree(&tree);
        tree_delete(&mut tree, 86);
        assert_valid_tree(&tree);
        assert_tree(&tree, vector[
            s(b"(3 Keys:[ 50 70 ] Children:[ 4 5 6 ] Leaf:f Prev:0 Next:0)"),
            s(b"(4 Keys:[ 10 40 ] Children:[ 8 9 10 ] Leaf:f Prev:0 Next:0)"),
            s(b"(5 Keys:[ 55 ] Children:[ 11 12 ] Leaf:f Prev:0 Next:0)"),
            s(b"(6 Keys:[ 100 105 ] Children:[ 15 16 17 ] Leaf:f Prev:0 Next:0)"),

            s(b"(8 Keys:[ 5 6 ] Values:[ 6 7 ] Children:[ 0 0 0 ] Leaf:t Prev:0 Next:9)"),
            s(b"(9 Keys:[ 11 12 ] Values:[ 14 15 ] Children:[ 0 0 0 ] Leaf:t Prev:8 Next:10)"),
            s(b"(10 Keys:[ 41 42 ] Values:[ 52 54 ] Children:[ 0 0 0 ] Leaf:t Prev:9 Next:11)"),
            s(b"(11 Keys:[ 54 55 ] Values:[ 69 70 ] Children:[ 0 0 0 ] Leaf:t Prev:10 Next:12)"),
            s(b"(12 Keys:[ 56 57 ] Values:[ 72 73 ] Children:[ 0 0 0 ] Leaf:t Prev:11 Next:15)"),
            s(b"(15 Keys:[ 76 77 87 ] Values:[ 97 99 111 ] Children:[ 0 0 0 0 ] Leaf:t Prev:12 Next:16)"),
            s(b"(16 Keys:[ 101 102 ] Values:[ 129 131 ] Children:[ 0 0 0 ] Leaf:t Prev:15 Next:17)"),
            s(b"(17 Keys:[ 109 110 ] Values:[ 140 141 ] Children:[ 0 0 0 ] Leaf:t Prev:16 Next:0)"),
        ]);
        assert_contains_keys(&tree, vector[
            5, 6,
            11, 12,
            41, 42,
            54, 55,
            56, 57,
            76, 77, 87,
            101, 102,
            109, 110,
        ]);
        destroy_tree(tree);
    }

    #[test]
    fun test_tree_delete_non_root_end_right_merge_rebalance_single_level_propogate() {
        // Tests that elements can be deleted from the end of a non root, triggering a right merge.
        // The merge should propogate only one additional level.

        let tree = tree_from_strs(4, vector[
            s(b"(3 Keys:[ 50 70 100 ] Children:[ 4 5 6 7 ] Leaf:f Prev:0 Next:0)"),
            s(b"(4 Keys:[ 10 40 ] Children:[ 8 9 10 ] Leaf:f Prev:0 Next:0)"),
            s(b"(5 Keys:[ 55 ] Children:[ 11 12 ] Leaf:f Prev:0 Next:0)"),
            s(b"(6 Keys:[ 85 ] Children:[ 14 15 ] Leaf:f Prev:0 Next:0)"),
            s(b"(7 Keys:[ 105 ] Children:[ 16 17 ] Leaf:f Prev:0 Next:0)"),

            s(b"(8 Keys:[ 5 6 ] Values:[ 6 7 ] Children:[ 0 0 0 ] Leaf:t Prev:0 Next:9)"),
            s(b"(9 Keys:[ 11 12 ] Values:[ 14 15 ] Children:[ 0 0 0 ] Leaf:t Prev:8 Next:10)"),
            s(b"(10 Keys:[ 41 42 ] Values:[ 52 54 ] Children:[ 0 0 0 ] Leaf:t Prev:9 Next:11)"),
            s(b"(11 Keys:[ 54 55 ] Values:[ 69 70 ] Children:[ 0 0 0 ] Leaf:t Prev:10 Next:12)"),
            s(b"(12 Keys:[ 56 57 ] Values:[ 72 73 ] Children:[ 0 0 0 ] Leaf:t Prev:11 Next:14)"),
            s(b"(14 Keys:[ 76 77 ] Values:[ 97 99 ] Children:[ 0 0 0 ] Leaf:t Prev:12 Next:15)"),
            s(b"(15 Keys:[ 86 87 ] Values:[ 110 111 ] Children:[ 0 0 0 ] Leaf:t Prev:14 Next:16)"),
            s(b"(16 Keys:[ 101 102 ] Values:[ 129 131 ] Children:[ 0 0 0 ] Leaf:t Prev:15 Next:17)"),
            s(b"(17 Keys:[ 109 110 ] Values:[ 140 141 ] Children:[ 0 0 0 ] Leaf:t Prev:16 Next:0)"),
        ]);
        assert_valid_tree(&tree);
        tree_delete(&mut tree, 77);
        assert_valid_tree(&tree);
        assert_tree(&tree, vector[
            s(b"(3 Keys:[ 50 70 ] Children:[ 4 5 6 ] Leaf:f Prev:0 Next:0)"),
            s(b"(4 Keys:[ 10 40 ] Children:[ 8 9 10 ] Leaf:f Prev:0 Next:0)"),
            s(b"(5 Keys:[ 55 ] Children:[ 11 12 ] Leaf:f Prev:0 Next:0)"),
            s(b"(6 Keys:[ 100 105 ] Children:[ 14 16 17 ] Leaf:f Prev:0 Next:0)"),

            s(b"(8 Keys:[ 5 6 ] Values:[ 6 7 ] Children:[ 0 0 0 ] Leaf:t Prev:0 Next:9)"),
            s(b"(9 Keys:[ 11 12 ] Values:[ 14 15 ] Children:[ 0 0 0 ] Leaf:t Prev:8 Next:10)"),
            s(b"(10 Keys:[ 41 42 ] Values:[ 52 54 ] Children:[ 0 0 0 ] Leaf:t Prev:9 Next:11)"),
            s(b"(11 Keys:[ 54 55 ] Values:[ 69 70 ] Children:[ 0 0 0 ] Leaf:t Prev:10 Next:12)"),
            s(b"(12 Keys:[ 56 57 ] Values:[ 72 73 ] Children:[ 0 0 0 ] Leaf:t Prev:11 Next:14)"),
            s(b"(14 Keys:[ 76 86 87 ] Values:[ 97 110 111 ] Children:[ 0 0 0 0 ] Leaf:t Prev:12 Next:16)"),
            s(b"(16 Keys:[ 101 102 ] Values:[ 129 131 ] Children:[ 0 0 0 ] Leaf:t Prev:14 Next:17)"),
            s(b"(17 Keys:[ 109 110 ] Values:[ 140 141 ] Children:[ 0 0 0 ] Leaf:t Prev:16 Next:0)"),
        ]);
        assert_contains_keys(&tree, vector[
            5, 6,
            11, 12,
            41, 42,
            54, 55,
            56, 57,
            76, 86, 87,
            101, 102,
            109, 110,
        ]);
        destroy_tree(tree);
    }

    #[test]
    fun test_tree_delete_non_root_end_left_merge_rebalance_single_level_propogate() {
        // Tests that elements can be deleted from the end of a non root, triggering a left merge.
        // The merge should propogate only one additional level.

        let tree = tree_from_strs(4, vector[
            s(b"(3 Keys:[ 50 70 100 ] Children:[ 4 5 6 7 ] Leaf:f Prev:0 Next:0)"),
            s(b"(4 Keys:[ 10 40 ] Children:[ 8 9 10 ] Leaf:f Prev:0 Next:0)"),
            s(b"(5 Keys:[ 55 ] Children:[ 11 12 ] Leaf:f Prev:0 Next:0)"),
            s(b"(6 Keys:[ 85 ] Children:[ 14 15 ] Leaf:f Prev:0 Next:0)"),
            s(b"(7 Keys:[ 105 ] Children:[ 16 17 ] Leaf:f Prev:0 Next:0)"),

            s(b"(8 Keys:[ 5 6 ] Values:[ 6 7 ] Children:[ 0 0 0 ] Leaf:t Prev:0 Next:9)"),
            s(b"(9 Keys:[ 11 12 ] Values:[ 14 15 ] Children:[ 0 0 0 ] Leaf:t Prev:8 Next:10)"),
            s(b"(10 Keys:[ 41 42 ] Values:[ 52 54 ] Children:[ 0 0 0 ] Leaf:t Prev:9 Next:11)"),
            s(b"(11 Keys:[ 54 55 ] Values:[ 69 70 ] Children:[ 0 0 0 ] Leaf:t Prev:10 Next:12)"),
            s(b"(12 Keys:[ 56 57 ] Values:[ 72 73 ] Children:[ 0 0 0 ] Leaf:t Prev:11 Next:14)"),
            s(b"(14 Keys:[ 76 77 ] Values:[ 97 99 ] Children:[ 0 0 0 ] Leaf:t Prev:12 Next:15)"),
            s(b"(15 Keys:[ 86 87 ] Values:[ 110 111 ] Children:[ 0 0 0 ] Leaf:t Prev:14 Next:16)"),
            s(b"(16 Keys:[ 101 102 ] Values:[ 129 131 ] Children:[ 0 0 0 ] Leaf:t Prev:15 Next:17)"),
            s(b"(17 Keys:[ 109 110 ] Values:[ 140 141 ] Children:[ 0 0 0 ] Leaf:t Prev:16 Next:0)"),
        ]);
        assert_valid_tree(&tree);
        tree_delete(&mut tree, 87);
        assert_valid_tree(&tree);
        assert_tree(&tree, vector[
            s(b"(3 Keys:[ 50 70 ] Children:[ 4 5 6 ] Leaf:f Prev:0 Next:0)"),
            s(b"(4 Keys:[ 10 40 ] Children:[ 8 9 10 ] Leaf:f Prev:0 Next:0)"),
            s(b"(5 Keys:[ 55 ] Children:[ 11 12 ] Leaf:f Prev:0 Next:0)"),
            s(b"(6 Keys:[ 100 105 ] Children:[ 15 16 17 ] Leaf:f Prev:0 Next:0)"),

            s(b"(8 Keys:[ 5 6 ] Values:[ 6 7 ] Children:[ 0 0 0 ] Leaf:t Prev:0 Next:9)"),
            s(b"(9 Keys:[ 11 12 ] Values:[ 14 15 ] Children:[ 0 0 0 ] Leaf:t Prev:8 Next:10)"),
            s(b"(10 Keys:[ 41 42 ] Values:[ 52 54 ] Children:[ 0 0 0 ] Leaf:t Prev:9 Next:11)"),
            s(b"(11 Keys:[ 54 55 ] Values:[ 69 70 ] Children:[ 0 0 0 ] Leaf:t Prev:10 Next:12)"),
            s(b"(12 Keys:[ 56 57 ] Values:[ 72 73 ] Children:[ 0 0 0 ] Leaf:t Prev:11 Next:15)"),
            s(b"(15 Keys:[ 76 77 86 ] Values:[ 97 99 110 ] Children:[ 0 0 0 0 ] Leaf:t Prev:12 Next:16)"),
            s(b"(16 Keys:[ 101 102 ] Values:[ 129 131 ] Children:[ 0 0 0 ] Leaf:t Prev:15 Next:17)"),
            s(b"(17 Keys:[ 109 110 ] Values:[ 140 141 ] Children:[ 0 0 0 ] Leaf:t Prev:16 Next:0)"),
        ]);
        assert_contains_keys(&tree, vector[
            5, 6,
            11, 12,
            41, 42,
            54, 55,
            56, 57,
            76, 77, 86,
            101, 102,
            109, 110,
        ]);
        destroy_tree(tree);
    }

    #[test]
    fun test_tree_delete_non_root_start_right_merge_rebalance_decrease_depth() {
        // Tests that elements can be deleted from the start of a non root, triggering a right merge.
        // The merge should decrease the depth of the tree.

        let tree = tree_from_strs(4, vector[
            s(b"(3 Keys:[ 100 ] Children:[ 6 7 ] Leaf:f Prev:0 Next:0)"),
            s(b"(6 Keys:[ 85 ] Children:[ 14 15 ] Leaf:f Prev:0 Next:0)"),
            s(b"(7 Keys:[ 105 ] Children:[ 16 17 ] Leaf:f Prev:0 Next:0)"),

            s(b"(14 Keys:[ 76 77 ] Values:[ 97 99 ] Children:[ 0 0 0 ] Leaf:t Prev:0 Next:15)"),
            s(b"(15 Keys:[ 86 87 ] Values:[ 110 111 ] Children:[ 0 0 0 ] Leaf:t Prev:14 Next:16)"),
            s(b"(16 Keys:[ 101 102 ] Values:[ 129 131 ] Children:[ 0 0 0 ] Leaf:t Prev:15 Next:17)"),
            s(b"(17 Keys:[ 109 110 ] Values:[ 140 141 ] Children:[ 0 0 0 ] Leaf:t Prev:16 Next:0)"),
        ]);
        assert_valid_tree(&tree);
        tree_delete(&mut tree, 76);
        assert_valid_tree(&tree);
        assert_tree(&tree, vector[
            s(b"(6 Keys:[ 100 105 ] Children:[ 14 16 17 ] Leaf:f Prev:0 Next:0)"),

            s(b"(14 Keys:[ 77 86 87 ] Values:[ 99 110 111 ] Children:[ 0 0 0 0 ] Leaf:t Prev:0 Next:16)"),
            s(b"(16 Keys:[ 101 102 ] Values:[ 129 131 ] Children:[ 0 0 0 ] Leaf:t Prev:14 Next:17)"),
            s(b"(17 Keys:[ 109 110 ] Values:[ 140 141 ] Children:[ 0 0 0 ] Leaf:t Prev:16 Next:0)"),
        ]);
        assert_contains_keys(&tree, vector[
            77, 86, 87,
            101, 102,
            109, 110,
        ]);
        destroy_tree(tree);
    }

    #[test]
    fun test_tree_delete_non_root_start_left_merge_rebalance_decrease_depth() {
        // Tests that elements can be deleted from the start of a non root, triggering a left merge.
        // The merge should decrease the depth of the tree.

        let tree = tree_from_strs(4, vector[
            s(b"(3 Keys:[ 100 ] Children:[ 6 7 ] Leaf:f Prev:0 Next:0)"),
            s(b"(6 Keys:[ 85 ] Children:[ 14 15 ] Leaf:f Prev:0 Next:0)"),
            s(b"(7 Keys:[ 105 ] Children:[ 16 17 ] Leaf:f Prev:0 Next:0)"),

            s(b"(14 Keys:[ 76 77 ] Values:[ 97 99 ] Children:[ 0 0 0 ] Leaf:t Prev:0 Next:15)"),
            s(b"(15 Keys:[ 86 87 ] Values:[ 110 111 ] Children:[ 0 0 0 ] Leaf:t Prev:14 Next:16)"),
            s(b"(16 Keys:[ 101 102 ] Values:[ 129 131 ] Children:[ 0 0 0 ] Leaf:t Prev:15 Next:17)"),
            s(b"(17 Keys:[ 109 110 ] Values:[ 140 141 ] Children:[ 0 0 0 ] Leaf:t Prev:16 Next:0)"),
        ]);
        assert_valid_tree(&tree);
        tree_delete(&mut tree, 86);
        assert_valid_tree(&tree);
        assert_tree(&tree, vector[
            s(b"(6 Keys:[ 100 105 ] Children:[ 15 16 17 ] Leaf:f Prev:0 Next:0)"),

            s(b"(15 Keys:[ 76 77 87 ] Values:[ 97 99 111 ] Children:[ 0 0 0 0 ] Leaf:t Prev:0 Next:16)"),
            s(b"(16 Keys:[ 101 102 ] Values:[ 129 131 ] Children:[ 0 0 0 ] Leaf:t Prev:15 Next:17)"),
            s(b"(17 Keys:[ 109 110 ] Values:[ 140 141 ] Children:[ 0 0 0 ] Leaf:t Prev:16 Next:0)"),
        ]);
        assert_contains_keys(&tree, vector[
            76, 77, 87,
            101, 102,
            109, 110,
        ]);
        destroy_tree(tree);
    }

    #[test]
    fun test_tree_delete_non_root_end_right_merge_rebalance_decrease_depth() {
        // Tests that elements can be deleted from the end of a non root, triggering a right merge.
        // The merge should decrease the depth of the tree.

        let tree = tree_from_strs(4, vector[
            s(b"(3 Keys:[ 100 ] Children:[ 6 7 ] Leaf:f Prev:0 Next:0)"),
            s(b"(6 Keys:[ 85 ] Children:[ 14 15 ] Leaf:f Prev:0 Next:0)"),
            s(b"(7 Keys:[ 105 ] Children:[ 16 17 ] Leaf:f Prev:0 Next:0)"),

            s(b"(14 Keys:[ 76 77 ] Values:[ 97 99 ] Children:[ 0 0 0 ] Leaf:t Prev:0 Next:15)"),
            s(b"(15 Keys:[ 86 87 ] Values:[ 110 111 ] Children:[ 0 0 0 ] Leaf:t Prev:14 Next:16)"),
            s(b"(16 Keys:[ 101 102 ] Values:[ 129 131 ] Children:[ 0 0 0 ] Leaf:t Prev:15 Next:17)"),
            s(b"(17 Keys:[ 109 110 ] Values:[ 140 141 ] Children:[ 0 0 0 ] Leaf:t Prev:16 Next:0)"),
        ]);
        assert_valid_tree(&tree);
        tree_delete(&mut tree, 77);
        assert_valid_tree(&tree);
        assert_tree(&tree, vector[
            s(b"(6 Keys:[ 100 105 ] Children:[ 14 16 17 ] Leaf:f Prev:0 Next:0)"),

            s(b"(14 Keys:[ 76 86 87 ] Values:[ 97 110 111 ] Children:[ 0 0 0 0 ] Leaf:t Prev:0 Next:16)"),
            s(b"(16 Keys:[ 101 102 ] Values:[ 129 131 ] Children:[ 0 0 0 ] Leaf:t Prev:14 Next:17)"),
            s(b"(17 Keys:[ 109 110 ] Values:[ 140 141 ] Children:[ 0 0 0 ] Leaf:t Prev:16 Next:0)"),
        ]);
        assert_contains_keys(&tree, vector[
            76, 86, 87,
            101, 102,
            109, 110,
        ]);
        destroy_tree(tree);
    }

    #[test]
    fun test_tree_delete_non_root_end_left_merge_rebalance_decrease_depth() {
        // Tests that elements can be deleted from the end of a non root, triggering a left merge.
        // The merge should decrease the depth of the tree.

        let tree = tree_from_strs(4, vector[
            s(b"(3 Keys:[ 100 ] Children:[ 6 7 ] Leaf:f Prev:0 Next:0)"),
            s(b"(6 Keys:[ 85 ] Children:[ 14 15 ] Leaf:f Prev:0 Next:0)"),
            s(b"(7 Keys:[ 105 ] Children:[ 16 17 ] Leaf:f Prev:0 Next:0)"),

            s(b"(14 Keys:[ 76 77 ] Values:[ 97 99 ] Children:[ 0 0 0 ] Leaf:t Prev:0 Next:15)"),
            s(b"(15 Keys:[ 86 87 ] Values:[ 110 111 ] Children:[ 0 0 0 ] Leaf:t Prev:14 Next:16)"),
            s(b"(16 Keys:[ 101 102 ] Values:[ 129 131 ] Children:[ 0 0 0 ] Leaf:t Prev:15 Next:17)"),
            s(b"(17 Keys:[ 109 110 ] Values:[ 140 141 ] Children:[ 0 0 0 ] Leaf:t Prev:16 Next:0)"),
        ]);
        assert_valid_tree(&tree);
        tree_delete(&mut tree, 87);
        assert_valid_tree(&tree);
        assert_tree(&tree, vector[
            s(b"(6 Keys:[ 100 105 ] Children:[ 15 16 17 ] Leaf:f Prev:0 Next:0)"),

            s(b"(15 Keys:[ 76 77 86 ] Values:[ 97 99 110 ] Children:[ 0 0 0 0 ] Leaf:t Prev:0 Next:16)"),
            s(b"(16 Keys:[ 101 102 ] Values:[ 129 131 ] Children:[ 0 0 0 ] Leaf:t Prev:15 Next:17)"),
            s(b"(17 Keys:[ 109 110 ] Values:[ 140 141 ] Children:[ 0 0 0 ] Leaf:t Prev:16 Next:0)"),
        ]);
        assert_contains_keys(&tree, vector[
            76, 77, 86,
            101, 102,
            109, 110,
        ]);
        destroy_tree(tree);
    }

    #[test]
    fun test_tree_delete_propogation_internal_first_child() {
        // Tests the case when the first child of an internal node is deleted after deleting its left child.

        let tree = tree_from_strs(4, vector[
            s(b"(3 Keys:[ 50 70 100 ] Children:[ 4 5 6 7 ] Leaf:f Prev:0 Next:0)"),
            s(b"(4 Keys:[ 10 40 ] Children:[ 8 9 10 ] Leaf:f Prev:0 Next:0)"),
            s(b"(5 Keys:[ 55 65 ] Children:[ 11 12 13 ] Leaf:f Prev:0 Next:0)"),
            s(b"(6 Keys:[ 85 ] Children:[ 14 15 ] Leaf:f Prev:0 Next:0)"),
            s(b"(7 Keys:[ 105 ] Children:[ 16 17 ] Leaf:f Prev:0 Next:0)"),

            s(b"(8 Keys:[ 5 6 ] Values:[ 6 7 ] Children:[ 0 0 0 ] Leaf:t Prev:0 Next:9)"),
            s(b"(9 Keys:[ 11 12 ] Values:[ 14 15 ] Children:[ 0 0 0 ] Leaf:t Prev:8 Next:10)"),
            s(b"(10 Keys:[ 41 42 ] Values:[ 52 54 ] Children:[ 0 0 0 ] Leaf:t Prev:9 Next:11)"),
            s(b"(11 Keys:[ 54 55 ] Values:[ 69 70 ] Children:[ 0 0 0 ] Leaf:t Prev:10 Next:12)"),
            s(b"(12 Keys:[ 56 57 ] Values:[ 72 73 ] Children:[ 0 0 0 ] Leaf:t Prev:11 Next:13)"),
            s(b"(13 Keys:[ 66 67 ] Values:[ 84 86 ] Children:[ 0 0 0 ] Leaf:t Prev:12 Next:14)"),
            s(b"(14 Keys:[ 76 77 ] Values:[ 97 99 ] Children:[ 0 0 0 ] Leaf:t Prev:13 Next:15)"),
            s(b"(15 Keys:[ 86 87 ] Values:[ 110 111 ] Children:[ 0 0 0 ] Leaf:t Prev:14 Next:16)"),
            s(b"(16 Keys:[ 101 102 ] Values:[ 129 131 ] Children:[ 0 0 0 ] Leaf:t Prev:15 Next:17)"),
            s(b"(17 Keys:[ 109 110 ] Values:[ 140 141 ] Children:[ 0 0 0 ] Leaf:t Prev:16 Next:0)"),
        ]);
        assert_valid_tree(&tree);
        tree_delete(&mut tree, 110);
        assert_valid_tree(&tree);
        assert_tree(&tree, vector[
            s(b"(3 Keys:[ 50 70 ] Children:[ 4 5 7 ] Leaf:f Prev:0 Next:0)"),
            s(b"(4 Keys:[ 10 40 ] Children:[ 8 9 10 ] Leaf:f Prev:0 Next:0)"),
            s(b"(5 Keys:[ 55 65 ] Children:[ 11 12 13 ] Leaf:f Prev:0 Next:0)"),
            s(b"(7 Keys:[ 85 100 ] Children:[ 14 15 17 ] Leaf:f Prev:0 Next:0)"),

            s(b"(8 Keys:[ 5 6 ] Values:[ 6 7 ] Children:[ 0 0 0 ] Leaf:t Prev:0 Next:9)"),
            s(b"(9 Keys:[ 11 12 ] Values:[ 14 15 ] Children:[ 0 0 0 ] Leaf:t Prev:8 Next:10)"),
            s(b"(10 Keys:[ 41 42 ] Values:[ 52 54 ] Children:[ 0 0 0 ] Leaf:t Prev:9 Next:11)"),
            s(b"(11 Keys:[ 54 55 ] Values:[ 69 70 ] Children:[ 0 0 0 ] Leaf:t Prev:10 Next:12)"),
            s(b"(12 Keys:[ 56 57 ] Values:[ 72 73 ] Children:[ 0 0 0 ] Leaf:t Prev:11 Next:13)"),
            s(b"(13 Keys:[ 66 67 ] Values:[ 84 86 ] Children:[ 0 0 0 ] Leaf:t Prev:12 Next:14)"),
            s(b"(14 Keys:[ 76 77 ] Values:[ 97 99 ] Children:[ 0 0 0 ] Leaf:t Prev:13 Next:15)"),
            s(b"(15 Keys:[ 86 87 ] Values:[ 110 111 ] Children:[ 0 0 0 ] Leaf:t Prev:14 Next:17)"),
            s(b"(17 Keys:[ 101 102 109 ] Values:[ 129 131 140 ] Children:[ 0 0 0 0 ] Leaf:t Prev:15 Next:0)"),
        ]);
        assert_contains_keys(&tree, vector[
            5, 6,
            11, 12,
            41, 42,
            54, 55,
            56, 57,
            66, 67,
            76, 77,
            86, 87,
            101, 102, 109,
        ]);
        destroy_tree(tree);
    }

    #[test]
    fun test_tree_delete_propogation_multiple_elements() {
        // Tests the case when there are multiple elements at each node when a delete is being propogated.

        let tree = tree_from_strs(6, vector[
            s(b"(3 Keys:[ 50 70 100 ] Children:[ 4 5 6 7 ] Leaf:f Prev:0 Next:0)"),
            s(b"(4 Keys:[ 10 40 ] Children:[ 8 9 10 ] Leaf:f Prev:0 Next:0)"),
            s(b"(5 Keys:[ 56 65 ] Children:[ 11 12 13 ] Leaf:f Prev:0 Next:0)"),
            s(b"(6 Keys:[ 85 95 ] Children:[ 14 15 16 ] Leaf:f Prev:0 Next:0)"),
            s(b"(7 Keys:[ 105 120 ] Children:[ 17 18 19 ] Leaf:f Prev:0 Next:0)"),

            s(b"(8 Keys:[ 5 6 7 ] Values:[ 10 12 14 ] Children:[ 0 0 0 0 ] Leaf:t Prev:0 Next:9)"),
            s(b"(9 Keys:[ 11 12 13 ] Values:[ 22 24 26 ] Children:[ 0 0 0 0 ] Leaf:t Prev:8 Next:10)"),
            s(b"(10 Keys:[ 41 42 43 ] Values:[ 82 84 86 ] Children:[ 0 0 0 0 ] Leaf:t Prev:9 Next:11)"),
            s(b"(11 Keys:[ 54 55 56 ] Values:[ 108 110 112 ] Children:[ 0 0 0 0 ] Leaf:t Prev:10 Next:12)"),
            s(b"(12 Keys:[ 57 58 59 ] Values:[ 114 116 118 ] Children:[ 0 0 0 0 ] Leaf:t Prev:11 Next:13)"),
            s(b"(13 Keys:[ 66 67 68 ] Values:[ 132 134 136 ] Children:[ 0 0 0 0 ] Leaf:t Prev:12 Next:14)"),
            s(b"(14 Keys:[ 76 77 78 ] Values:[ 152 154 156 ] Children:[ 0 0 0 0 ] Leaf:t Prev:13 Next:15)"),
            s(b"(15 Keys:[ 86 87 88 ] Values:[ 172 174 176 ] Children:[ 0 0 0 0 ] Leaf:t Prev:14 Next:16)"),
            s(b"(16 Keys:[ 96 97 98 ] Values:[ 192 194 196 ] Children:[ 0 0 0 0 ] Leaf:t Prev:15 Next:17)"),
            s(b"(17 Keys:[ 101 102 103 ] Values:[ 202 204 206 ] Children:[ 0 0 0 0 ] Leaf:t Prev:16 Next:18)"),
            s(b"(18 Keys:[ 109 110 111 ] Values:[ 218 220 222 ] Children:[ 0 0 0 0 ] Leaf:t Prev:17 Next:19)"),
            s(b"(19 Keys:[ 121 122 123 ] Values:[ 242 244 246 ] Children:[ 0 0 0 0 ] Leaf:t Prev:18 Next:0)"),
        ]);
        assert_valid_tree(&tree);
        tree_delete(&mut tree, 110);
        assert_valid_tree(&tree);
        assert_tree(&tree, vector[
            s(b"(3 Keys:[ 50 70 ] Children:[ 4 5 7 ] Leaf:f Prev:0 Next:0)"),
            s(b"(4 Keys:[ 10 40 ] Children:[ 8 9 10 ] Leaf:f Prev:0 Next:0)"),
            s(b"(5 Keys:[ 56 65 ] Children:[ 11 12 13 ] Leaf:f Prev:0 Next:0)"),
            s(b"(7 Keys:[ 85 95 100 105 ] Children:[ 14 15 16 17 18 ] Leaf:f Prev:0 Next:0)"),

            s(b"(8 Keys:[ 5 6 7 ] Values:[ 10 12 14 ] Children:[ 0 0 0 0 ] Leaf:t Prev:0 Next:9)"),
            s(b"(9 Keys:[ 11 12 13 ] Values:[ 22 24 26 ] Children:[ 0 0 0 0 ] Leaf:t Prev:8 Next:10)"),
            s(b"(10 Keys:[ 41 42 43 ] Values:[ 82 84 86 ] Children:[ 0 0 0 0 ] Leaf:t Prev:9 Next:11)"),
            s(b"(11 Keys:[ 54 55 56 ] Values:[ 108 110 112 ] Children:[ 0 0 0 0 ] Leaf:t Prev:10 Next:12)"),
            s(b"(12 Keys:[ 57 58 59 ] Values:[ 114 116 118 ] Children:[ 0 0 0 0 ] Leaf:t Prev:11 Next:13)"),
            s(b"(13 Keys:[ 66 67 68 ] Values:[ 132 134 136 ] Children:[ 0 0 0 0 ] Leaf:t Prev:12 Next:14)"),
            s(b"(14 Keys:[ 76 77 78 ] Values:[ 152 154 156 ] Children:[ 0 0 0 0 ] Leaf:t Prev:13 Next:15)"),
            s(b"(15 Keys:[ 86 87 88 ] Values:[ 172 174 176 ] Children:[ 0 0 0 0 ] Leaf:t Prev:14 Next:16)"),
            s(b"(16 Keys:[ 96 97 98 ] Values:[ 192 194 196 ] Children:[ 0 0 0 0 ] Leaf:t Prev:15 Next:17)"),
            s(b"(17 Keys:[ 101 102 103 ] Values:[ 202 204 206 ] Children:[ 0 0 0 0 ] Leaf:t Prev:16 Next:18)"),
            s(b"(18 Keys:[ 109 111 121 122 123 ] Values:[ 218 222 242 244 246 ] Children:[ 0 0 0 0 0 0 ] Leaf:t Prev:17 Next:0)"),
        ]);
        assert_contains_keys(&tree, vector[
            5, 6, 7,
            11, 12, 13,
            41, 42, 43,
            54, 55, 56,
            57, 58, 59,
            66, 67, 68,
            76, 77, 78,
            86, 87, 88,
            96, 97, 98,
            101, 102, 103,
            109, 111, 121, 122, 123,
        ]);
        destroy_tree(tree);
    }

    // Utility methods.

    #[test_only]
    fun destroy_tree<T: copy + store + drop>(tree: Tree<T>) {
        let Tree {
            root: _,
            treeSize: _,
            nodes,
            m: _,
            currNodeID: _,
            unusedNodeStack: _,
            min: _,
            max: _,
        } = tree;
        table::drop_unchecked(nodes);
    }

    // Test harness utils.

    #[test_only]
    fun build_tree(m: u64, keys: vector<u64>, values: vector<u16>): Tree<u16> {
        let i = 0;
        let size = vector::length(&keys);
        assert!(vector::length(&values) == size, 0);
        let tree = new_tree(m);
        while (i < size) {
            let key = *vector::borrow(&keys, i);
            let value = *vector::borrow(&values, i);
            tree_insert(&mut tree, key, value);
            i = i + 1;
        };
        tree
    }

    // Generates a tree with [1, count] elements, inserted in random order.
    // Returns the elements added to the tree.
    #[test_only]
    fun gen_tree_sequential_random_order(m: u64, count: u64): (Tree<u16>, vector<u64>) {
        let keys = ftu::gen_sequential_list(count, 0);
        let values = ftu::convert_u64_list_to_u16(&keys);
        vector::reverse(&mut values);
        let keysCpy = *&keys;
        // Shuffle the elements generated.
        let i = 0;
        while (i < count) {
            let i3 = (i+1) * (i+1) * (i+1);
            let idx = 18446744073709551615 % i3 % count;
            vector::swap(&mut keys, i, idx);
            i = i + 1;
        };
        (build_tree(m, keys, values), keysCpy)
    }

    // Generates a tree with [1, count] elements, inserted in increasing order.
    // Returns the elements added to the tree.
    #[test_only]
    fun gen_tree_sequential_increasing_order(m: u64, count: u64): (Tree<u16>, vector<u64>) {
        let keys = ftu::gen_sequential_list(count, 0);
        let values = ftu::convert_u64_list_to_u16(&keys);
        vector::reverse(&mut values);
        let keysCpy = *&keys;
        (build_tree(m, keys, values), keysCpy)
    }

    // Generates a tree with [1, count] elements, inserted in decreasing order.
    // Returns the elements added to the tree.
    #[test_only]
    fun gen_tree_sequential_decreasing_order(m: u64, count: u64): (Tree<u16>, vector<u64>) {
        // Generates a tree with `count` elements in increasing order.
        let keys = ftu::gen_sequential_list(count, 0);
        let values = ftu::convert_u64_list_to_u16(&keys);
        vector::reverse(&mut values);
        let keysCpy = *&keys;
        vector::reverse(&mut keys);
        (build_tree(m, keys, values), keysCpy)
    }

    // Assertion utils.

    #[test_only]
    fun assert_contains_range(tree: &Tree<u16>, start: u64, end: u64) {
        assert!(start != 0 && start < end, 0);
        // Check increasing direction.
        let it = tree_iterate(tree, INCREASING_ITERATOR);
        let expected = start;
        while (it.pos.nodeID != 0) {
            let (key, _) = tree_get_next(tree, &mut it);
            assert!(expected == key, 0);
            expected = expected + 1;
        };
        assert!(expected == end + 1, 0);
        // Also check decreasing direction.
        let it = tree_iterate(tree, DECREASING_ITERATOR);
        let expected = end;
        while (it.pos.nodeID != 0) {
            let (key, _) = tree_get_next(tree, &mut it);
            assert!(expected == key, 0);
            expected = expected - 1;
        };
        assert!(expected == start - 1, 0);
    }

    #[test_only]
    fun assert_contains_keys(tree: &Tree<u16>, keys: vector<u64>) {
        let keySet = table::new();
        let i = 0;
        while (i < vector::length(&keys)) {
            table::add(&mut keySet, *vector::borrow(&keys, i), true);
            i = i + 1;
        };

        // Check increasing direction.
        let count = 0;
        let it = tree_iterate(tree, INCREASING_ITERATOR);
        while (it.pos.nodeID != 0) {
            let (key, _) = tree_get_next(tree, &mut it);
            assert!(table::contains(&keySet, key), 0);
            count = count + 1;
        };
        assert!(count == vector::length(&keys), 0);
        // Also check the decreasing direction.
        count = 0;
        it = tree_iterate(tree, DECREASING_ITERATOR);
        while (it.pos.nodeID != 0) {
            let (key, _z) = tree_get_next(tree, &mut it);
            assert!(table::contains(&keySet, key), 0);
            count = count + 1;
        };
        assert!(count == vector::length(&keys), 0);

        table::drop_unchecked(keySet);
    }

    #[test_only]
    fun assert_tree(tree: &Tree<u16>, expected: vector<string::String>) {
        let levels = table::new();
        node_str(&mut levels, tree, tree.root, 0);
        let i = 0;
        let out = vector::empty();
        while (table::contains(&levels, i)) {
            vector::append(&mut out, *table::borrow(&levels, i));
            i = i + 1;
        };
        assert!(out == expected, 0);
        table::drop_unchecked(levels);
    }

    #[test_only]
    fun assert_valid_tree<T: copy + store + drop>(tree: &Tree<T>) {
        // Make sure that each nodes follows B tree constraints.
        let encounteredNodes = twl::new();
        let encounteredValues = twl::new();
        let leafCount = 0;

        if (tree.root != 0) {
            assert_valid_node(
                tree,
                &mut encounteredNodes,
                &mut encounteredValues,
                &mut leafCount,
                true,
                tree.root,
                0,
                MAX_U64,
            );
        };

        assert!(twl::length(&encounteredValues) == tree.treeSize, 0);

        // Validate leaf nodes from min to max direction.
        let currLeafNode = tree.min;
        let prevValue = 0;
        let elemCount = 0;
        while (currLeafNode != 0) {
            let node = table::borrow(&tree.nodes, currLeafNode);
            let i = 0;
            let size = vector::length(&node.elements);
            assert!(node.firstChild == 0, 0);
            while (i < size) {
                let elem = vector::borrow(&node.elements, i);
                assert!(elem.key > prevValue, 0);
                prevValue = elem.key;
                assert!(elem.child == 0, 0);
                i = i + 1;
                elemCount = elemCount + 1;
            };
            currLeafNode = node.next;
        };
        assert!(elemCount == tree.treeSize, 0);
        // Validate leaf nodes from max to min direction.
        currLeafNode = tree.max;
        prevValue = 0;
        elemCount = 0;
        while (currLeafNode != 0) {
            let node = table::borrow(&tree.nodes, currLeafNode);
            let i = 0;
            let size = vector::length(&node.elements);
            assert!(node.firstChild == 0, 0);
            while (i < size) {
                let elem = vector::borrow(&node.elements, size - i - 1);
                if (prevValue != 0) {
                    assert!(elem.key < prevValue, 0);
                };
                prevValue = elem.key;
                assert!(elem.child == 0, 0);
                i = i + 1;
                elemCount = elemCount + 1;
            };
            currLeafNode = node.prev;
        };
        assert!(elemCount == tree.treeSize, 0);

        twl::drop_unchecked(encounteredNodes);
        twl::drop_unchecked(encounteredValues);
    }

    #[test_only]
    fun assert_valid_node<T: copy + store + drop>(
        tree: &Tree<T>,
        encounteredNodes: &mut twl::TableWithLength<u16, bool>,
        encounteredValues: &mut twl::TableWithLength<u64, bool>,
        leafCount: &mut u16,
        root: bool,
        nodeID: u16,
        minRange: u64,
        maxRange: u64,
    ) {
        assert!(!twl::contains(encounteredNodes, nodeID), 0);
        twl::add(encounteredNodes, nodeID, true);

        let node = table::borrow(&tree.nodes, nodeID);
        let isLeaf = node.firstChild == 0;

        let i = 0;
        let size = vector::length(&node.elements);
        let minSize = if (root) {
            1
        } else if (isLeaf) {
            tree.m/2
        } else {
            tree.m/2 - 1
        };
        assert!(size >= minSize, 0);
        assert!(size <= tree.m, 0);

        let prevElem = 0;
        while (i < size) {
            let elem = vector::borrow(&node.elements, i);
            assert!(elem.key > prevElem, 0); // This also checks to ensure no element value is a 0.
            prevElem = elem.key;
            assert!(elem.key > minRange, 0);
            assert!(elem.key <= maxRange, 0);

            if (isLeaf) {
                assert!(elem.child == 0, 0);
                assert!(!twl::contains(encounteredValues, elem.key), 0);
                twl::add(encounteredValues, elem.key, true);
            } else {
                assert!(table::contains(&tree.nodes, elem.child), 0);
                assert!(!twl::contains(encounteredNodes, elem.child), 0);
            };

            i = i + 1;
        };

        if (isLeaf) {
            *leafCount = *leafCount + 1;
            if (node.next != 0) {
                assert!(table::contains(&tree.nodes, node.next), 0);
            };
            if (node.prev != 0) {
                assert!(table::contains(&tree.nodes, node.prev), 0);
            };
            let i = 0;
            while (i < size) {
                let elem = vector::borrow(&node.elements, i);
                assert!(vector::length(&elem.value) == 1, 0); // Leaf nodes should have a value set.
                i = i + 1;
            };
        } else {
            assert!(node.next == 0, 0);
            assert!(node.prev == 0, 0);

            // Resursive call.
            assert_valid_node(
                tree,
                encounteredNodes,
                encounteredValues,
                leafCount,
                false,
                node.firstChild,
                minRange,
                vector::borrow(&node.elements, 0).key,
            );
            let i = 0;
            while (i < size) {
                let elem = vector::borrow(&node.elements, i);
                assert!(vector::length(&elem.value) == 0, 0); // Internal nodes should have no values set.
                assert_valid_node(
                    tree,
                    encounteredNodes,
                    encounteredValues,
                    leafCount,
                    false,
                    elem.child,
                    elem.key,
                    if (i < size - 1) {
                        vector::borrow(&node.elements, i+1).key
                    } else {
                        maxRange
                    },
                );
                i = i + 1;
            }
        }
    }

    #[test]
    fun test_tree_from_strs() {
        let strs = vector<string::String>[
            s(b"(3 Keys:[ 5 ] Children:[ 1 2 ] Leaf:f Prev:0 Next:0)"),
            s(b"(1 Keys:[ 1 2 3 4 5 ] Values:[ 5 4 3 2 1 ] Children:[ 0 0 0 0 0 0 ] Leaf:t Prev:0 Next:2)"),
            s(b"(2 Keys:[ 6 7 8 9 10 ] Values:[ 10 9 8 7 6 ] Children:[ 0 0 0 0 0 0 ] Leaf:t Prev:1 Next:0)"),
        ];
        let tree = tree_from_strs(8, strs);
        assert!(tree.treeSize == 10, 0);
        assert!(tree.root == 3, 0);
        assert!(tree.min == 1, 0);
        assert!(tree.max == 2, 0);
        assert!(tree.currNodeID == 4, 0);
        assert!(tree.m == 8, 0);
        assert!(table::borrow(&tree.nodes, 3) == &TreeNode {
            firstChild: 1,
            elements: vector<TreeElem<u16>>[
                TreeElem { key: 5, value: vector[], child: 2},
            ],
            next: 0,
            prev: 0,
        }, 0);
        assert!(table::borrow(&tree.nodes, 1) == &TreeNode {
            firstChild: 0,
            elements: vector<TreeElem<u16>>[
                TreeElem { key: 1, value: vector[5], child: 0},
                TreeElem { key: 2, value: vector[4], child: 0},
                TreeElem { key: 3, value: vector[3], child: 0},
                TreeElem { key: 4, value: vector[2], child: 0},
                TreeElem { key: 5, value: vector[1], child: 0},
            ],
            next: 2,
            prev: 0,
        }, 0);
        assert!(table::borrow(&tree.nodes, 2) == &TreeNode {
            firstChild: 0,
            elements: vector<TreeElem<u16>>[
                TreeElem { key: 6, value: vector[10], child: 0},
                TreeElem { key: 7, value: vector[9], child: 0},
                TreeElem { key: 8, value: vector[8], child: 0},
                TreeElem { key: 9, value: vector[7], child: 0},
                TreeElem { key: 10, value: vector[6], child: 0},
            ],
            next: 0,
            prev: 1,
        }, 0);
        assert_valid_tree(&tree);
        destroy_tree(tree);
    }

    #[test]
    fun test_node_from_str() {
        let str = s(b"(182 Keys:[ 996 997 998 999 1000 ] Values:[ 1 2 3 4 5 ] Children:[ 0 0 0 0 0 0 ] Leaf:t Prev:84 Next:0)");
        let (nodeID, node) = node_from_str(&str);
        assert!(nodeID == 182, 0);
        assert!(node.prev == 84, 0);
        assert!(node.next == 0, 0);
        assert!(node.firstChild == 0, 0);
        ftu::assert_vector_equal(&node.elements, &vector<TreeElem<u16>>[
            TreeElem { key: 996, value: vector[1], child: 0},
            TreeElem { key: 997, value: vector[2], child: 0},
            TreeElem { key: 998, value: vector[3], child: 0},
            TreeElem { key: 999, value: vector[4], child: 0},
            TreeElem { key: 1000, value: vector[5], child: 0},
        ]);

        str = s(b"(60 Keys:[ 211 438 604 732 872 ] Children:[ 12 122 59 181 112 207 ] Leaf:f Prev:0 Next:0)");
        (nodeID, node) = node_from_str(&str);
        assert!(nodeID == 60, 0);
        assert!(node.prev == 0, 0);
        assert!(node.next == 0, 0);
        assert!(node.firstChild == 12, 0);
        ftu::assert_vector_equal(&node.elements, &vector<TreeElem<u16>>[
            TreeElem { key: 211, value: vector[], child: 122},
            TreeElem { key: 438, value: vector[], child: 59},
            TreeElem { key: 604, value: vector[], child: 181},
            TreeElem { key: 732, value: vector[], child: 112},
            TreeElem { key: 872, value: vector[], child: 207},
        ]);
    }

    // String to struct utils.

    const LEFT_BRACKET: u8 = 0x28;
    const RIGHT_BRACKET: u8 = 0x29;
    const LEFT_SQ_BRACKET: u8 = 0x5b;
    const RIGHT_SQ_BRACKET: u8 = 0x5d;
    const COLON: u8 = 0x3a;
    const SPACE: u8 = 0x20;
    const T: u8 = 0x74;
    const F: u8 = 0x66;

    #[test_only]
    fun tree_from_strs(m: u64, strs: vector<string::String>): Tree<u16> {
        let nodes = table::new();
        let i = 0;
        let (elemCount, maxNodeID, root, min, minElem, max, maxElem) = (0, 0, 0, 0, 0, 0, 0);
        let size = vector::length(&strs);
        while (i < size) {
            let (nodeID, node) = node_from_str(vector::borrow(&strs, i));
            assert!(!table::contains(&nodes, nodeID), 0);
            if (root == 0) {
                root = nodeID;
            };
            if (maxNodeID < nodeID) {
                maxNodeID = nodeID;
            };
            if (node.firstChild == 0) {
                let nodeElems = vector::length(&node.elements);
                let firstElem = vector::borrow(&node.elements, 0);
                let lastElem = vector::borrow(&node.elements, nodeElems - 1);
                elemCount = elemCount + nodeElems;
                if (maxElem < lastElem.key) {
                    maxElem = lastElem.key;
                    max = nodeID;
                };
                if (minElem == 0 || minElem > firstElem.key) {
                    minElem = firstElem.key;
                    min = nodeID;
                };
            };

            table::add(&mut nodes, nodeID, node);
            i = i + 1;
        };

        Tree {
            root,
            nodes,
            treeSize: elemCount,
            m,
            unusedNodeStack: 0,
            currNodeID: maxNodeID + 1,
            min,
            max,
        }
    }

    #[test_only]
    fun node_from_str(str: &string::String): (u16, TreeNode<u16>) {
        let bytes = string::bytes(str);
        let i = 0;
        assert!(*vector::borrow(bytes, i) == LEFT_BRACKET, 0);
        i = i + 1;
        // Read u64 value;
        let j = i;
        while (*vector::borrow(bytes, i) != SPACE) { i = i + 1; };
        let nodeID = ftu::u16_from_bytes(string::bytes(&string::sub_string(str, j, i)));
        // Read Keys array.
        i = i + 1;
        let keyword = b"";
        while (*vector::borrow(bytes, i) != COLON) { vector::push_back(&mut keyword, *vector::borrow(bytes, i)); i = i + 1; };
        assert!(keyword == b"Keys", 0);
        j = i + 1;
        while (*vector::borrow(bytes, i) != RIGHT_SQ_BRACKET) { i = i + 1; };
        i = i + 1;
        let keys = ftu::u64_vector_from_str(&string::sub_string(str, j, i));
        let size = vector::length(&keys);
        // Read childrens or values array.
        i = i + 1;
        keyword = b"";
        while (*vector::borrow(bytes, i) != COLON) { vector::push_back(&mut keyword, *vector::borrow(bytes, i)); i = i + 1; };
        let values = if (keyword == b"Values") {
            j = i + 1;
            while (*vector::borrow(bytes, i) != RIGHT_SQ_BRACKET) { i = i + 1; };
            i = i + 1;
            let values = ftu::u16_vector_from_str(&string::sub_string(str, j, i));
            assert!(size == vector::length(&values), 0);
            i = i + 1;
            keyword = b"";
            while (*vector::borrow(bytes, i) != COLON) { vector::push_back(&mut keyword, *vector::borrow(bytes, i)); i = i + 1; };
            values
        } else {
            vector[]
        };
        assert!(keyword == b"Children", 0);
        j = i + 1;
        while (*vector::borrow(bytes, i) != RIGHT_SQ_BRACKET) { i = i + 1; };
        i = i + 1;
        let children = ftu::u16_vector_from_str(&string::sub_string(str, j, i));
        assert!(size + 1 == vector::length(&children), 0);
        // Read in leaf prop.
        i = i + 1;
        keyword = b"";
        while (*vector::borrow(bytes, i) != COLON) { vector::push_back(&mut keyword, *vector::borrow(bytes, i)); i = i + 1; };
        assert!(keyword == b"Leaf", 0);
        i = i + 1;
        let expectedIsLeaf = if (*vector::borrow(&children, 0) == 0) {
            T
        } else {
            F
        };
        assert!(*vector::borrow(bytes, i) == expectedIsLeaf, 0);
        // Read in node prev.
        i = i + 2;
        keyword = b"";
        while (*vector::borrow(bytes, i) != COLON) { vector::push_back(&mut keyword, *vector::borrow(bytes, i)); i = i + 1; };
        assert!(keyword == b"Prev", 0);
        j = i + 1;
        while (*vector::borrow(bytes, i) != SPACE) { i = i + 1; };
        let prev = ftu::u16_from_bytes(string::bytes(&string::sub_string(str, j, i)));
        // Read in node next.
        i = i + 1;
        keyword = b"";
        while (*vector::borrow(bytes, i) != COLON) { vector::push_back(&mut keyword, *vector::borrow(bytes, i)); i = i + 1; };
        assert!(keyword == b"Next", 0);
        j = i + 1;
        while (*vector::borrow(bytes, i) != RIGHT_BRACKET) { i = i + 1; };
        let next = ftu::u16_from_bytes(string::bytes(&string::sub_string(str, j, i)));

        let elems = vector::empty<TreeElem<u16>>();
        i = 0;
        let isLeaf = *vector::borrow(&children, 0) == 0;
        if (isLeaf) {
            assert!(vector::length(&values) > 0, 0); // Leafs must have values defined.
        } else {
            assert!(vector::length(&values) == 0, 0); // Internal nodes must have no values defined.
        };
        while (i < size) {
            vector::push_back(&mut elems, TreeElem {
                key: *vector::borrow(&keys, i),
                value: if (isLeaf) {
                    vector[*vector::borrow(&values, i)]
                } else {
                    vector[]
                },
                child: *vector::borrow(&children, i+1),
            });
            i = i + 1;
        };
        (nodeID, TreeNode {
            elements: elems,
            firstChild: *vector::borrow(&children, 0),
            next,
            prev,
        })
    }

    // Struct to string utils.

    #[test_only]
    fun node_str(
        levels: &mut table::Table<u64,
            vector<string::String>>,
        tree: &Tree<u16>,
        nodeID: u16,
        level: u64,
    ) {
        if (nodeID == 0) {
            return
        };
        let node = table::borrow(&tree.nodes, nodeID);
        let nodeStr = single_node_str(nodeID, node);
        let levelList = table::borrow_mut_with_default(levels, level, vector::empty());
        vector::push_back(levelList, nodeStr);
        node_str(levels, tree, node.firstChild, level+1);
        let i = 0;
        let size = vector::length(&node.elements);
        while (i < size) {
            node_str(levels, tree, vector::borrow(&node.elements, i).child, level+1);
            i = i + 1;
        };
    }

    #[test_only]
    fun single_node_str(id: u16, node: &TreeNode<u16>): string::String {
        let out = s(b"(");
        string::append(&mut out, ftu::u16_to_string(id));
        let (keyStr, valuesStr, childrenStr) = elems_to_string(node.firstChild, &node.elements);
        string::append(&mut out, s(b" Keys:"));
        string::append(&mut out, keyStr);
        if (node.firstChild == 0) {
            string::append(&mut out, s(b" Values:"));
            string::append(&mut out, valuesStr);
        };
        string::append(&mut out, s(b" Children:"));
        string::append(&mut out, childrenStr);
        string::append(&mut out, s(b" Leaf:"));
        string::append(&mut out, ftu::bool_to_string(node.firstChild == 0));
        string::append(&mut out, s(b" Prev:"));
        string::append(&mut out, ftu::u16_to_string(node.prev));
        string::append(&mut out, s(b" Next:"));
        string::append(&mut out, ftu::u16_to_string(node.next));
        string::append(&mut out, s(b")"));
        out
    }

    #[test_only]
    fun elems_to_string(firstChild: u16, vec: &vector<TreeElem<u16>>): (string::String, string::String, string::String) {
        let keys = vector[];
        let values = vector[];
        let children = vector[firstChild];
        let i = 0;
        let size = vector::length(vec);
        let isLeaf = firstChild == 0;

        while (i < size) {
            let elem = vector::borrow(vec, i);
            vector::push_back(&mut keys, elem.key);
            if (isLeaf) {
                assert!(vector::length(&elem.value) == 1, 0);
            } else {
                assert!(vector::length(&elem.value) == 0, 0);
            };
            if (isLeaf) {
                vector::push_back(&mut values,*vector::borrow(&elem.value, 0))
            };
            vector::push_back(&mut children, elem.child);
            i = i + 1;
        };
        (
            ftu::u64_vector_to_str(&keys),
            ftu::u16_vector_to_str(&values),
            ftu::u16_vector_to_str(&children),
        )
    }

    // Debugging utils.

    #[test_only]
    fun print_tree(tree: &Tree<u16>) {
        let levels = table::new();
        node_str(&mut levels, tree, tree.root, 0);
        let i = 0;
        let out = s(b"\n");
        while (table::contains(&levels, i)) {
            let levelStr = ftu::u64_to_string(i);
            string::append_utf8(&mut levelStr, b": ");
            let level = table::borrow(&levels, i);
            let j = 0;
            while (j < vector::length(level)) {
                string::append(&mut levelStr, *vector::borrow(level, j));
                string::append_utf8(&mut levelStr, b"\n   ");
                j = j + 1;
            };
            string::append_utf8(&mut levelStr, b"\n");
            string::append(&mut out, levelStr);
            i = i + 1;
        };
        table::drop_unchecked(levels);
        debug::print(&out);
    }

    #[test_only]
    fun print_single_node(nodeID: u16, node: &TreeNode<u16>) {
        debug::print(&single_node_str(nodeID, node));
    }

    // </editor-fold>

    // </editor-fold>
}