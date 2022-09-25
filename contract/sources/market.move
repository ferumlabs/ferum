module ferum::market {
    use aptos_framework::coin;
    use aptos_std::event::{EventHandle, emit_event};
    use aptos_framework::account::{new_event_handle};
    use aptos_framework::timestamp;
    use aptos_std::table;
    use aptos_std::type_info;
    use std::vector;
    use std::signer::address_of;
    use std::string::{Self, String};

    use ferum::admin::{register_market, get_market_addr};
    use ferum::custodian::{CustodianCapability, get_custodian_address, is_custodian_address_valid};
    #[test_only]
    use ferum::admin::{init_ferum};
    #[test_only]
    use ferum::custodian::{register_custodian, drop_custodian_capability};
    use ferum_std::fixed_point_64::{Self, FixedPoint64, from_u64};
    #[test_only]
    use ferum::coin_test_helpers::{FMA, FMB, setup_fake_coins, register_fmb, register_fma, create_fake_coins};
    use ferum_std::math::min_u8;
    use ferum_std::red_black_tree;
    use ferum_std::red_black_tree::{Tree, has_next_value, get_next_value, is_empty, max_key, min_key};
    #[test_only]
    use aptos_framework::account;
    use ferum_std::linked_list;

    //
    // Errors
    //

    const ERR_NOT_ALLOWED: u64 = 1;
    const ERR_NOT_ADMIN: u64 = 2;
    const ERR_BOOK_EXISTS: u64 = 3;
    const ERR_BOOK_DOES_NOT_EXIST: u64 = 4;
    const ERR_COIN_UNINITIALIZED: u64 = 5;
    const ERR_UNKNOWN_ORDER: u64 = 6;
    const ERR_INVALID_PRICE: u64 = 7;
    const ERR_NOT_OWNER: u64 = 8;
    const ERR_COIN_EXCEEDS_MAX_SUPPORTED_DECIMALS: u64 = 9;
    const ERR_INVALID_TYPE: u64 = 10;
    const ERR_NO_PROGRESS: u64 = 11;
    const ERR_MARKET_ORDER_NOT_PENDING: u64 = 12;
    const ERR_INVALID_DECIMAL_CONFIG: u64 = 13;
    const ERR_INVALID_SIDE: u64 = 14;
    const ERR_CLORDID_TOO_LARGE: u64 = 15;
    const ERR_NOT_CUSTODIAN: u64 = 16;
    const ERR_INVALID_CUSTODIAN_ADDRESS: u64 = 17;

    //
    // Enums.
    //

    // Represents a sell order.
    const SIDE_SELL: u8 = 1;
    // Represents a buy order.
    const SIDE_BUY: u8 = 2;

    // Represents a market order.
    const TYPE_MARKET: u8 = 1;
    // Represents a limit order.
    const TYPE_LIMIT: u8 = 2;

    // Represents a pending order.
    const STATUS_PENDING: u8 = 1;
    // Represents a cancelled order.
    const STATUS_CANCELLED: u8 = 2;
    // Represents a partially filled order.
    const STATUS_PARTIALLY_FILLED: u8 = 3;
    // Represents a filled order.
    const STATUS_FILLED: u8 = 4;

    // Used as the default value, ie: the order was not cancelled.
    const CANCEL_AGENT_NONE: u8 = 0;
    // Used when the order is cancelled because it was an IOC order.
    const CANCEL_AGENT_IOC: u8 = 1;
    // Used when the order was cancelled by the user (or custodian).
    const CANCEL_AGENT_USER: u8 = 2;

    //
    // Constants.
    //

    const MAX_DECIMALS: u8 = 10;

    //
    // Structs.
    //

    // Struct representing id for an order.
    struct OrderID has copy, drop, store {
        owner: address, // Address for the owner of the order.
        counter: u128, // Unique counter value for this owner's order.
    }

    // Market specific user information.
    struct UserMarketInfo<phantom I, phantom Q> has key {
        // Used the generate the next order ID.
        idCounter: u128,
    }

    struct OrderMetadata has drop, store, copy {
        // Type info for the instrument coin type for the order.
        instrumentType: type_info::TypeInfo,
        // Type info for the quote coin type for the order.
        quoteType: type_info::TypeInfo,
        // Side for this order. See the OrderSide enum.
        side: u8,
        // Remaining quantity for this order.
        remainingQty: FixedPoint64,
        // The original quantity for this order.
        originalQty: FixedPoint64,
        // Limit price for this order.
        price: FixedPoint64,
        // Type for this order. See the OrderType enum.
        type: u8,
        // Status of this order. See the OrderStatus enum.
        status: u8,
        // Optional metadata provided for this order.
        clientOrderID: String,
        // Internal counter used to derive an execution id.
        executionCounter: u128,
        // Internal counter used to make event parsing easier:
        // an event with a higher update counter is more up to date
        // than an event with a lower counter.
        updateCounter: u128,
    }

    struct Order<phantom I, phantom Q> has store {
        // Unique identifier for this order.
        id: OrderID,
        // The order's metadata.
        metadata: OrderMetadata,
        // Remaining buy collateral for this order.
        buyCollateral: coin::Coin<Q>,
        // Remaining sell collateral for this order.
        sellCollateral: coin::Coin<I>,
        // Address of the custodian that placed this order.
        // If not placed via a custodian, will be the sentinal value of @0x0.
        custodianAddress: address,
    }

    struct OrderBook<phantom I, phantom Q> has key, store {
        // Order IDs of marker orders.
        marketOrders: vector<OrderID>,
        // Order IDs stored in order of decreasing price.
        sells_tree: red_black_tree::Tree<OrderID>,
        // Order IDs stored in order of increasing price.
        buys_tree: red_black_tree::Tree<OrderID>,
        // Map of all non finalized orders.
        orderMap: table::Table<OrderID, Order<I, Q>>,
        // Map of all finalized orders.
        finalizedOrderMap: table::Table<OrderID, Order<I, Q>>,
        // Number of decimals for the instrument coin.
        iDecimals: u8,
        // Number of decimals for the quote coin.
        qDecimals: u8,
        // Structure describing the fees for this market.
        fees: FeeStructure,
        // Finalize order events for this market.
        finalizeEvents: EventHandle<FinalizeEvent>,
        // Execution events for this market.
        executionEvents: EventHandle<ExecutionEvent>,
        // Create order events for this market.
        createOrderEvents: EventHandle<CreateEvent>,
        // Price update events for this market.
        priceUpdateEvents: EventHandle<PriceUpdateEvent>,
    }

    // Struct encapsulating price at a given timestamp for the market.
    struct Quote has drop, store {
        // Type info for the instrument coin type for the order.
        instrumentType: type_info::TypeInfo,
        // Type info for the quote coin type for the order.
        quoteType: type_info::TypeInfo,
        // The most someone is willing to pay for the given instrument/quote pair.
        maxBid: FixedPoint64,
        // How much quantity there is the the maxBid price point.
        bidSize: FixedPoint64,
        // The least someone is willing to accept as payment for the given instrument/quote pair.
        minAsk: FixedPoint64,
        // How much quantity there is the the minAsk price point.
        askSize: FixedPoint64,
        // The chain timestamp this quote was issued at.
        timestampMicroSeconds: u64
    }

    struct FeeStructure has store {
        takerFeeBps: FixedPoint64,
        makerFeeBps: FixedPoint64,
        custodianFeeBps: FixedPoint64,
        tiers: vector<FeeTier>,
    }

    struct FeeTier has store {
        maxFerumTokens: u128,
        makerFeeBps: FixedPoint64,
        takerFeeBps: FixedPoint64,
    }

    //
    // Events.
    //

    struct ExecutionEvent has drop, store {
        orderID: OrderID,
        orderMetadata: OrderMetadata,
        oppositeOrderID: OrderID,
        oppositeOrderMetadata: OrderMetadata,
        price: FixedPoint64,
        qty: FixedPoint64,
        timestampMicroSeconds: u64
    }

    struct FinalizeEvent has drop, store {
        orderID: OrderID,
        orderMetadata: OrderMetadata,
        cancelAgent: u8,
        timestampMicroSeconds: u64
    }

    struct CreateEvent has drop, store {
        orderID: OrderID,
        orderMetadata: OrderMetadata,
        timestampMicroSeconds: u64
    }

    struct PriceUpdateEvent has drop, store {
        data: Quote,
    }

    //
    // Entry functions.
    //

    public entry fun init_market_entry<I, Q>(owner: &signer, instrumentDecimals: u8, quoteDecimals: u8) {
        let ownerAddr = address_of(owner);
        assert!(!exists<OrderBook<I, Q>>(ownerAddr), ERR_BOOK_EXISTS);
        let (iCoinDecimals, qCoinDecimals) = validate_coins<I, Q>();
        assert!(iCoinDecimals >= instrumentDecimals && qCoinDecimals >= quoteDecimals, ERR_INVALID_DECIMAL_CONFIG);
        assert!(instrumentDecimals + quoteDecimals <= min_u8(iCoinDecimals, qCoinDecimals), ERR_INVALID_DECIMAL_CONFIG);

        let finalizeEvents = new_event_handle<FinalizeEvent>(owner);
        let createOrderEvents = new_event_handle<CreateEvent>(owner);
        let executionEvents = new_event_handle<ExecutionEvent>(owner);
        let priceUpdateEvents = new_event_handle<PriceUpdateEvent>(owner);
        // TODO: add facility for setting fee structures for a market.
        let fees = FeeStructure{
            takerFeeBps: fixed_point_64::zero(),
            makerFeeBps: fixed_point_64::zero(),
            custodianFeeBps: fixed_point_64::zero(),
            tiers: vector::empty(),
        };

        let book = OrderBook<I, Q>{
            marketOrders: vector::empty(),
            sells_tree: red_black_tree::new<OrderID>(),
            buys_tree: red_black_tree::new<OrderID>(),
            orderMap: table::new<OrderID, Order<I, Q>>(),
            finalizedOrderMap: table::new<OrderID, Order<I, Q>>(),
            iDecimals: instrumentDecimals,
            qDecimals: quoteDecimals,
            fees,
            finalizeEvents,
            createOrderEvents,
            executionEvents,
            priceUpdateEvents,
        };
        move_to(owner, book);
        register_market<I, Q>(ownerAddr);
    }

    public entry fun add_limit_order_entry<I, Q>(
        owner: &signer,
        side: u8,
        price: u64,
        qty: u64,
        clientOrderID: String,
    ) acquires OrderBook, UserMarketInfo {
        let book = borrow_global<OrderBook<I, Q>>(get_market_addr<I, Q>());
        let priceFixedPoint = from_u64(price, book.qDecimals);
        let qtyFixedPoint = from_u64(qty, book.iDecimals);

        add_limit_order<I, Q>(owner, side, priceFixedPoint, qtyFixedPoint, clientOrderID);
    }

    public entry fun add_market_order_entry<I, Q>(
        owner: &signer,
        side: u8,
        qty: u64,
        maxCollateralAmt: u64,
        clientOrderID: String,
    ) acquires OrderBook, UserMarketInfo {
        let book = borrow_global<OrderBook<I, Q>>(get_market_addr<I, Q>());
        let qtyFixedPoint = from_u64(qty, book.iDecimals);

        add_market_order<I, Q>(owner, side, qtyFixedPoint, maxCollateralAmt, clientOrderID);
    }

    public entry fun cancel_order_entry<I, Q>(owner: &signer, orderIDCounter: u128) acquires OrderBook {
        let id = OrderID {
            owner: address_of(owner),
            counter: orderIDCounter,
        };
        cancel_order_internal<I, Q>(owner, id, @0x0);
    }

    //
    // Public functions.
    //

    public fun add_limit_order<I, Q>(
        owner: &signer,
        side: u8,
        price: FixedPoint64,
        qty: FixedPoint64,
        clientOrderID: String,
    ): OrderID acquires OrderBook, UserMarketInfo {
        add_limit_order_internal<I, Q>(owner, side, price, qty, clientOrderID, @0x0)
    }

    public fun add_limit_order_as_custodian<I, Q>(
        owner: &signer,
        custodianCap: &CustodianCapability,
        side: u8,
        price: FixedPoint64,
        qty: FixedPoint64,
        clientOrderID: String,
    ): OrderID acquires OrderBook, UserMarketInfo {
        let custodianAddress = get_custodian_address(custodianCap);
        assert!(is_custodian_address_valid(custodianAddress), ERR_INVALID_CUSTODIAN_ADDRESS);

        add_limit_order_internal<I, Q>(
            owner,
            side,
            price,
            qty,
            clientOrderID,
            custodianAddress,
        )
    }

    public fun add_market_order<I, Q>(
        owner: &signer,
        side: u8,
        qty: FixedPoint64,
        maxCollateralAmt: u64,
        clientOrderID: String,
    ): OrderID acquires OrderBook, UserMarketInfo {
        add_market_order_internal<I, Q>(
            owner,
            side,
            qty,
            maxCollateralAmt,
            clientOrderID,
            @0x0,
        )
    }

    public fun add_market_order_as_custodian<I, Q>(
        owner: &signer,
        custodianCap: &CustodianCapability,
        side: u8,
        qty: FixedPoint64,
        maxCollateralAmt: u64,
        clientOrderID: String,
    ): OrderID acquires OrderBook, UserMarketInfo {
        let custodianAddress = get_custodian_address(custodianCap);
        assert!(is_custodian_address_valid(custodianAddress), ERR_INVALID_CUSTODIAN_ADDRESS);
        add_market_order_internal<I, Q>(
            owner,
            side,
            qty,
            maxCollateralAmt,
            clientOrderID,
            custodianAddress,
        )
    }

    public fun cancel_order_as_custodian<I, Q>(
        signer: &signer,
        custodianCap: &CustodianCapability,
        orderOwner: address,
        orderIDCounter: u128,
    ) acquires OrderBook {
        let custodianAddress = get_custodian_address(custodianCap);
        let id = OrderID {
            owner: orderOwner,
            counter: orderIDCounter,
        };
        cancel_order_internal<I, Q>(signer, id, custodianAddress)
    }

    public fun get_limit_order_collateral_amount<I, Q>(
        side: u8,
        price: FixedPoint64,
        qty: FixedPoint64,
    ): (FixedPoint64, FixedPoint64) {
        if (side == SIDE_BUY) {
            (
                fixed_point_64::multiply_round_up(price, qty),
                fixed_point_64::zero(),
            )
        } else {
            (
                fixed_point_64::zero(),
                qty,
            )
        }
    }

    public fun get_market_order_collateral_amount<I, Q>(
        side: u8,
        qty: FixedPoint64,
        maxCollateralAmt: u64,
    ): (FixedPoint64, FixedPoint64) {
        if (side == SIDE_BUY) {
            (from_u64(maxCollateralAmt, coin::decimals<Q>()), fixed_point_64::zero())
        } else {
            (fixed_point_64::zero(), qty)
        }
    }

    public fun get_market_decimals<I, Q>(): (u8, u8) acquires OrderBook {
        let book = borrow_global<OrderBook<I, Q>>(get_market_addr<I, Q>());
        (book.iDecimals, book.qDecimals)
    }

    //
    // Private functions.
    //

    fun add_limit_order_internal<I, Q>(
        owner: &signer,
        side: u8,
        price: FixedPoint64,
        qty: FixedPoint64,
        clientOrderID: String,
        custodianAddress: address,
    ): OrderID acquires OrderBook, UserMarketInfo {
        let bookAddr = get_market_addr<I, Q>();
        assert!(exists<OrderBook<I, Q>>(bookAddr), ERR_BOOK_DOES_NOT_EXIST);
        validate_coins<I, Q>();
        create_user_info_if_needed<I, Q>(owner);

        let ownerAddr = address_of(owner);
        let book = borrow_global_mut<OrderBook<I, Q>>(bookAddr);

        // Validates that the decimal places don't exceed the max decimal places allowed by the market.
        fixed_point_64::to_u128(price, book.qDecimals);
        fixed_point_64::to_u128(qty, book.iDecimals);

        let (buyCollateral, sellCollateral) = obtain_limit_order_collateral_internal<I, Q>(
            owner,
            side,
            price,
            qty,
        );

        let orderID = gen_order_id<I, Q>(ownerAddr);
        let order = Order<I, Q>{
            id: orderID,
            buyCollateral,
            sellCollateral,
            custodianAddress,
            metadata: OrderMetadata{
                instrumentType: type_info::type_of<I>(),
                quoteType: type_info::type_of<Q>(),
                side,
                price,
                remainingQty: qty,
                type: TYPE_LIMIT,
                originalQty: qty,
                status: STATUS_PENDING,
                clientOrderID,
                executionCounter: 0,
                updateCounter: 0,
            },
        };
        add_order<I, Q>(book, order);
        orderID
    }

    fun add_market_order_internal<I, Q>(
        owner: &signer,
        side: u8,
        qty: FixedPoint64,
        maxCollateralAmt: u64,
        clientOrderID: String,
        custodianAddress: address,
    ): OrderID acquires OrderBook, UserMarketInfo {
        let bookAddr = get_market_addr<I, Q>();
        assert!(exists<OrderBook<I, Q>>(bookAddr), ERR_BOOK_DOES_NOT_EXIST);
        validate_coins<I, Q>();
        create_user_info_if_needed<I, Q>(owner);
        let book = borrow_global_mut<OrderBook<I, Q>>(bookAddr);
        let ownerAddr = address_of(owner);

        // Validates that the decimal places don't exceed the max decimal places allowed by the market.
        fixed_point_64::to_u128(qty, book.iDecimals);

        let (buyCollateral, sellCollateral) = obtain_market_order_collateral_internal<I, Q>(
            owner,
            side,
            qty,
            maxCollateralAmt,
        );

        let orderID = gen_order_id<I, Q>(ownerAddr);
        let order = Order<I, Q>{
            id: orderID,
            buyCollateral,
            sellCollateral,
            custodianAddress,
            metadata: OrderMetadata{
                instrumentType: type_info::type_of<I>(),
                quoteType: type_info::type_of<Q>(),
                side,
                price: fixed_point_64::zero(),
                remainingQty: qty,
                type: TYPE_MARKET,
                originalQty: qty,
                status: STATUS_PENDING,
                clientOrderID,
                executionCounter: 0,
                updateCounter: 0,
            },
        };
        add_order(book, order);
        orderID
    }

    fun cancel_order_internal<I, Q>(owner: &signer, orderID: OrderID, custodianAddress: address) acquires OrderBook {
        let bookAddr = get_market_addr<I, Q>();
        assert!(exists<OrderBook<I, Q>>(bookAddr), ERR_BOOK_DOES_NOT_EXIST);

        let ownerAddr = address_of(owner);
        let book = borrow_global_mut<OrderBook<I, Q>>(bookAddr);
        assert!(table::contains(&book.orderMap, orderID), ERR_UNKNOWN_ORDER);
        let order = table::borrow_mut(&mut book.orderMap, orderID);

        if (is_custodian_address_valid(order.custodianAddress)) {
            assert!(custodianAddress == order.custodianAddress, ERR_NOT_CUSTODIAN);
        } else {
            assert!(ownerAddr == order.id.owner, ERR_NOT_OWNER);
        };

        mark_cancelled_order(
            &mut book.finalizeEvents,
            order,
            CANCEL_AGENT_USER,
        );

        clean_order(order.id, &mut book.orderMap, &mut book.finalizedOrderMap);
    }

    fun gen_order_id<I, Q>(owner: address): OrderID acquires UserMarketInfo {
        let market_info = borrow_global_mut<UserMarketInfo<I, Q>>(owner);
        let counter = market_info.idCounter;
        market_info.idCounter = market_info.idCounter + 1;
        OrderID {
            owner,
            counter,
        }
    }

    fun add_order<I, Q>(book: &mut OrderBook<I, Q>, order: Order<I, Q>) {
        validate_order(&order);
        emit_order_created_event(book, &order);

        if (order.metadata.type == TYPE_MARKET) {
            vector::push_back(&mut book.marketOrders, order.id);
        } else if (order.metadata.side == SIDE_BUY) {
            red_black_tree::insert(&mut book.buys_tree, fixed_point_64::value(order.metadata.price), order.id);
        } else {
            red_black_tree::insert(&mut book.sells_tree, fixed_point_64::value(order.metadata.price), order.id);
        };

        let orderMap = &mut book.orderMap;
        table::add(orderMap, order.id, order);

        process_orders(book)
    }

    fun process_orders<I, Q>(book: &mut OrderBook<I, Q>) {
        let buysTree = &mut book.buys_tree;
        let sellsTree = &mut book.sells_tree;
        let marketOrders = &book.marketOrders;
        let orderMap = &mut book.orderMap;
        // Process market orders first.
        if (vector::length(marketOrders) > 0) {
            let i = vector::length(marketOrders);
            while (i > 0) {
                let order = get_order_from_list_mut(orderMap, marketOrders, i - 1);
                if (order.metadata.side == SIDE_BUY) {
                    execute_market_order(
                        &mut book.executionEvents,
                        &mut book.finalizeEvents,
                        orderMap,
                        sellsTree,
                        order.id,
                    )
                } else {
                    execute_market_order(
                        &mut book.executionEvents,
                        &mut book.finalizeEvents,
                        orderMap,
                        buysTree,
                        order.id,
                    )
                };
                i = i - 1;
            }
        };

        clean_market_orders(&mut book.orderMap, &mut book.finalizedOrderMap, &mut book.marketOrders);

        // If there are no orders in any one side, we can return.
        if (is_empty(&book.buys_tree) || is_empty(&book.sells_tree)) {
            // Update the price before we do return.
            let data = get_quote(book);
            emit_event(&mut book.priceUpdateEvents, PriceUpdateEvent{
                data,
            });
            return
        };

        execute_limit_orders(book);

        // Update the price before returning.
        let data = get_quote(book);
        emit_event(&mut book.priceUpdateEvents, PriceUpdateEvent{
            data,
        });
    }

    fun get_order_from_list_mut<I, Q>(
        orderMap: &mut table::Table<OrderID, Order<I, Q>>,
        list: &vector<OrderID>,
        i: u64,
    ): &mut Order<I, Q> {
        let orderID = *vector::borrow(list, i);
        table::borrow_mut(orderMap, orderID)
    }

    fun get_order_from_list<I, Q>(
        orderMap: &table::Table<OrderID, Order<I, Q>>,
        list: &vector<OrderID>,
        i: u64,
    ): &Order<I, Q> {
        let orderID = *vector::borrow(list, i);
        table::borrow(orderMap, orderID)
    }

    fun execute_limit_orders<I, Q>(book: &mut OrderBook<I, Q>) {
        let orderMap = &mut book.orderMap;
        let timestampMicroSeconds = timestamp::now_microseconds();

        // Iterate until either of the sides is empty or until the lowest ask is higher than the highest bid.
        while (!(is_empty(&book.sells_tree) || is_empty(&book.buys_tree)) &&
               max_key(&book.buys_tree) >= min_key(&book.sells_tree)) {

            let maxBidKey = max_key(&book.buys_tree);
            let maxBidPrice = fixed_point_64::new_u128(maxBidKey);
            let maxBidID = *red_black_tree::first_value_at(&book.buys_tree, maxBidKey);
            let maxBid = table::borrow(orderMap, maxBidID);

            let minAskKey = min_key(&book.sells_tree);
            let minAskPrice = fixed_point_64::new_u128(minAskKey);
            let minAskID = *red_black_tree::first_value_at(&book.sells_tree, minAskKey);
            let minAsk = table::borrow(orderMap, minAskID);

            // Shouldn't need to worry about over executing collateral because the minAsk price is less than the
            // maxBid price.
            let executedQty = fixed_point_64::min(maxBid.metadata.remainingQty, minAsk.metadata.remainingQty);

            let maxBidMut = table::borrow_mut(orderMap, maxBidID);
            maxBidMut.metadata.updateCounter = maxBidMut.metadata.updateCounter + 1;
            maxBidMut.metadata.executionCounter = maxBidMut.metadata.executionCounter + 1;
            maxBidMut.metadata.remainingQty = fixed_point_64::sub(maxBidMut.metadata.remainingQty, executedQty);

            let minAskMut = table::borrow_mut(orderMap, minAskID);
            minAskMut.metadata.updateCounter = minAskMut.metadata.updateCounter + 1;
            minAskMut.metadata.executionCounter = minAskMut.metadata.executionCounter + 1;
            minAskMut.metadata.remainingQty = fixed_point_64::sub(minAskMut.metadata.remainingQty, executedQty);

            // Give the midpoint for the price.
            // TODO: compute feed using market fee structure.
            let price = fixed_point_64::divide_round_up(
                fixed_point_64::add(minAskPrice, maxBidPrice),
                from_u64(2, 0),
            );
            // Its possible for the midpoint to have more decimal places than the market allows for quotes.
            // In this case, round up.
            price = fixed_point_64::round_up_to_decimals(price, book.qDecimals);

            swap_collateral(orderMap, maxBidID, minAskID, price, executedQty);

            // Update status of orders.
            let maxBidMut = table::borrow_mut(orderMap, maxBidID);
            let maxBidFinalized = finalize_order_if_needed(&mut book.finalizeEvents, maxBidMut);
            let maxBidMetadata = maxBidMut.metadata;
            let minAskMut = table::borrow_mut(orderMap, minAskID);
            let minAskFinalized = finalize_order_if_needed(&mut book.finalizeEvents, minAskMut);
            let minAskMetadata = minAskMut.metadata;

            // Emit execution events after having modified what we need to.
            emit_event(&mut book.executionEvents, ExecutionEvent {
                orderID: maxBidID,
                orderMetadata: maxBidMetadata,
                oppositeOrderID: minAskID,
                oppositeOrderMetadata: minAskMetadata,
                price,
                qty: executedQty,
                timestampMicroSeconds,
            });
            emit_event(&mut book.executionEvents, ExecutionEvent {
                orderID: minAskID,
                orderMetadata: minAskMetadata,
                oppositeOrderID: maxBidID,
                oppositeOrderMetadata: maxBidMetadata,
                price,
                qty: executedQty,
                timestampMicroSeconds,
            });

            if (maxBidFinalized) {
                clean_order(maxBidID, orderMap, &mut book.finalizedOrderMap);
                red_black_tree::delete_value(&mut book.buys_tree, maxBidKey, maxBidID);
            };

            if (minAskFinalized) {
                clean_order(minAskID, orderMap, &mut book.finalizedOrderMap);
                red_black_tree::delete_value(&mut book.sells_tree, minAskKey, minAskID);
            };
        };
    }

    fun execute_market_order<I, Q>(
        execution_event_handle: &mut EventHandle<ExecutionEvent>,
        finalize_event_handle: &mut EventHandle<FinalizeEvent>,
        orderMap: &mut table::Table<OrderID, Order<I, Q>>,
        orderTree: &mut red_black_tree::Tree<OrderID>,
        orderID: OrderID,
    ) {
        let order =  table::borrow_mut(orderMap, orderID);
        assert!(order.metadata.status == STATUS_PENDING, ERR_MARKET_ORDER_NOT_PENDING);
        if (is_empty(orderTree)) {
            mark_cancelled_order(finalize_event_handle, order, CANCEL_AGENT_IOC);
            return
        };
        let timestampMicroSeconds = timestamp::now_microseconds();
        let isOrderFinalized = false;

        let orderTreeIterator =
            if (order.metadata.side == SIDE_BUY)
                red_black_tree::min_iterator(orderTree)
            else
                red_black_tree::max_iterator(orderTree);

        // Iterate until the current order is finalized or there are no more orders left in the book.
        while (!isOrderFinalized && has_next_value(&orderTreeIterator)) {
            let bookOrderID = get_next_value(orderTree, &mut orderTreeIterator);
            let executedQty = {
                let order =  table::borrow(orderMap, orderID);
                let bookOrder = table::borrow(orderMap, bookOrderID);
                if (order.metadata.side == SIDE_BUY) {
                    // For a buy order, we need to factor in the remaining collateral for the order when deciding what
                    // the execution qty should be.
                    let remainingCollateral = get_remaining_collateral(order);
                    let maxQtyAtPrice = fixed_point_64::divide_trunc(remainingCollateral, bookOrder.metadata.price);
                    let remainingQty =  fixed_point_64::min(
                        bookOrder.metadata.remainingQty,
                        order.metadata.remainingQty,
                    );
                    fixed_point_64::min(maxQtyAtPrice, remainingQty)
                } else {
                    // For a sell order, we can just use the remaining quantity.
                    fixed_point_64::min(
                        bookOrder.metadata.remainingQty,
                        order.metadata.remainingQty,
                    )
                }
            };

            let bookOrder = table::borrow_mut(orderMap, bookOrderID);
            bookOrder.metadata.updateCounter = bookOrder.metadata.updateCounter + 1;
            bookOrder.metadata.executionCounter = bookOrder.metadata.executionCounter + 1;
            bookOrder.metadata.remainingQty = fixed_point_64::sub(bookOrder.metadata.remainingQty, executedQty);
            let bookOrderID = bookOrder.id;
            let bookOrderPrice = bookOrder.metadata.price;

            let order = table::borrow_mut(orderMap, orderID);
            order.metadata.updateCounter = order.metadata.updateCounter + 1;
            order.metadata.executionCounter = order.metadata.executionCounter + 1;
            order.metadata.remainingQty = fixed_point_64::sub(order.metadata.remainingQty, executedQty);
            let orderID = order.id;

            if (order.metadata.side == SIDE_BUY) {
                swap_collateral(
                    orderMap,
                    orderID,
                    bookOrderID,
                    bookOrderPrice,
                    executedQty,
                );
            } else {
                swap_collateral(
                    orderMap,
                    bookOrderID,
                    orderID,
                    bookOrderPrice,
                    executedQty,
                );
            };

            // Update status of orders.
            let bookOrder = table::borrow_mut(orderMap, bookOrderID);
            finalize_order_if_needed(finalize_event_handle, bookOrder);
            let bookOrderMetadata = bookOrder.metadata;
            let order = table::borrow_mut(orderMap, orderID);
            isOrderFinalized = finalize_order_if_needed(finalize_event_handle, order);
            let orderMetadata = order.metadata;

            // Emit execution events.
            emit_event(execution_event_handle, ExecutionEvent {
                orderID,
                orderMetadata,
                oppositeOrderID: bookOrderID,
                oppositeOrderMetadata: bookOrderMetadata,
                price: bookOrderMetadata.price,
                qty: executedQty,
                timestampMicroSeconds,
            });
            emit_event(execution_event_handle, ExecutionEvent {
                orderID: bookOrderID,
                orderMetadata: bookOrderMetadata,
                oppositeOrderID: orderID,
                oppositeOrderMetadata: orderMetadata,
                price: bookOrderMetadata.price,
                qty: executedQty,
                timestampMicroSeconds,
            });
        };

        let order =  table::borrow_mut(orderMap, orderID);
        if (order.metadata.status != STATUS_FILLED && order.metadata.status != STATUS_PARTIALLY_FILLED) {
            mark_cancelled_order(finalize_event_handle, order, CANCEL_AGENT_IOC);
        }
    }

    fun clean_market_orders<I, Q>(
        orderMap: &mut table::Table<OrderID, Order<I, Q>>,
        finalizedOrderMap: &mut table::Table<OrderID, Order<I, Q>>,
        orderList: &mut vector<OrderID>,
    ) {
        let i = vector::length(orderList);
        while (i > 0) {
            let orderID = *vector::borrow(orderList, i - 1);
            if (clean_order(orderID, orderMap, finalizedOrderMap)) {
                vector::remove(orderList, i - 1);
            };
            i = i - 1;
        }
    }

    // Returns true if given order is finalized.
    fun clean_order<I, Q>(
        orderID: OrderID,
        orderMap: &mut table::Table<OrderID, Order<I, Q>>,
        finalizedOrderMap: &mut table::Table<OrderID, Order<I, Q>>) : bool {
        let order = table::borrow(orderMap, orderID);
        let isFinalized = (
            order.metadata.status == STATUS_FILLED ||
            order.metadata.status == STATUS_PARTIALLY_FILLED ||
            order.metadata.status == STATUS_CANCELLED
        );
        if (isFinalized) {
            let orderOwner = order.id.owner;
            let order = table::remove(orderMap, orderID);

            let buyCollateral = coin::extract_all(&mut order.buyCollateral);
            let sellCollateral = coin::extract_all(&mut order.sellCollateral);

            table::add(finalizedOrderMap, order.id, order);

            // Return any remaining collateral to user.
            coin::deposit(orderOwner, buyCollateral);
            coin::deposit(orderOwner, sellCollateral);
        };
        isFinalized
    }

    fun get_quote<I, Q>(book: &OrderBook<I, Q>): Quote {
        let timestamp = timestamp::now_microseconds();

        let zero = fixed_point_64::zero();
        let bidSize = zero;
        let askSize = zero;
        let minAsk = zero;
        let maxBid = zero;

        if (!is_empty(&book.buys_tree)) {
            maxBid = fixed_point_64::new_u128(red_black_tree::max_key(&book.buys_tree));
            bidSize = get_size(&book.orderMap, &book.buys_tree, maxBid);
        };

        if (!is_empty(&book.sells_tree)) {
            minAsk = fixed_point_64::new_u128(red_black_tree::min_key(&book.sells_tree));
            askSize = get_size(&book.orderMap, &book.sells_tree, minAsk);
        };

        Quote {
            instrumentType: type_info::type_of<I>(),
            quoteType: type_info::type_of<Q>(),
            minAsk,
            askSize,
            maxBid,
            bidSize,
            timestampMicroSeconds: timestamp,
        }
    }

    fun get_size<I, Q>(
        orderMap: &table::Table<OrderID, Order<I, Q>>,
        orderTree: &Tree<OrderID>,
        price: FixedPoint64,
    ): FixedPoint64 {
        let sum = fixed_point_64::zero();
        let ordersList = red_black_tree::values_at_list(orderTree, fixed_point_64::value(price));
        let ordersListIterator = linked_list::iterator(ordersList);
        while (linked_list::has_next(&ordersListIterator)) {
            let orderID = linked_list::get_next(ordersList,&mut ordersListIterator);
            let order = table::borrow(orderMap, orderID);
            sum = fixed_point_64::add(sum, order.metadata.remainingQty);
        };
        sum
    }

    //
    // Validation functions.
    //

    fun validate_order<I, Q>(order: &Order<I, Q>) {
        let metadata = &order.metadata;
        if (metadata.type == TYPE_MARKET) {
            assert!(fixed_point_64::eq(metadata.price, fixed_point_64::zero()), ERR_INVALID_PRICE);
        }
        else if (metadata.type == TYPE_LIMIT) {
            assert!(fixed_point_64::gt(metadata.price, fixed_point_64::zero()), ERR_INVALID_PRICE);
        };
        assert!(metadata.side == SIDE_BUY || metadata.side == SIDE_SELL, ERR_INVALID_SIDE);
        assert!(string::length(&metadata.clientOrderID) < 40, ERR_CLORDID_TOO_LARGE);

    }

    fun validate_coins<I, Q>(): (u8, u8) {
        let iDecimals = coin::decimals<I>();
        let qDecimals = coin::decimals<Q>();
        assert!(coin::is_coin_initialized<Q>(), ERR_COIN_UNINITIALIZED);
        assert!(qDecimals <= MAX_DECIMALS, ERR_COIN_EXCEEDS_MAX_SUPPORTED_DECIMALS);
        assert!(coin::is_coin_initialized<I>(), ERR_COIN_UNINITIALIZED);
        assert!(iDecimals <= MAX_DECIMALS, ERR_COIN_EXCEEDS_MAX_SUPPORTED_DECIMALS);
        (iDecimals, qDecimals)
    }

    //
    // Order specific helpers.
    //

    fun finalize_order_if_needed<I, Q>(
        finalize_event_handle: &mut EventHandle<FinalizeEvent>,
        order: &mut Order<I, Q>,
    ): bool {
        let hasCollateral = has_remaining_collateral(order);
        let hasQty = has_remaining_qty(order);
        if (!hasCollateral || !hasQty) {
            order.metadata.status = if (hasQty) STATUS_PARTIALLY_FILLED else STATUS_FILLED;
            order.metadata.updateCounter = order.metadata.updateCounter + 1;
            emit_event(finalize_event_handle, FinalizeEvent{
                orderID: order.id,
                orderMetadata: order.metadata,
                cancelAgent: CANCEL_AGENT_NONE,

                timestampMicroSeconds: timestamp::now_microseconds(),
            });
            return true
        };
        false
    }

    fun emit_order_created_event<I, Q>(book: &mut OrderBook<I, Q>, order: &Order<I, Q>) {
        emit_event(&mut book.createOrderEvents, CreateEvent{
            orderID: order.id,
            orderMetadata: order.metadata,
            timestampMicroSeconds: timestamp::now_microseconds(),
        });
    }

    fun has_remaining_qty<I, Q>(order: &Order<I, Q>): bool {
        !fixed_point_64::eq(order.metadata.remainingQty, fixed_point_64::zero())
    }

    fun has_remaining_collateral<I, Q>(order: &Order<I, Q>): bool {
        coin::value(&order.buyCollateral) > 0 || coin::value(&order.sellCollateral) > 0
    }

    fun get_remaining_collateral<I, Q>(order: &Order<I, Q>): FixedPoint64 {
        if (order.metadata.side == SIDE_BUY) {
            let coinDecimals = coin::decimals<Q>();
            from_u64(coin::value(&order.buyCollateral), coinDecimals)
        } else {
            let coinDecimals = coin::decimals<I>();
            from_u64(coin::value(&order.sellCollateral), coinDecimals)
        }
    }

    public fun mark_cancelled_order<I, Q>(
        finalize_event_handle: &mut EventHandle<FinalizeEvent>,
        order: &mut Order<I, Q>,
        cancelAgent: u8,
    ) {
        order.metadata.status = STATUS_CANCELLED;
        order.metadata.updateCounter = order.metadata.updateCounter + 1;
        emit_event(finalize_event_handle, FinalizeEvent{
            orderID: order.id,
            orderMetadata: order.metadata,
            cancelAgent,
            timestampMicroSeconds: timestamp::now_microseconds(),
        })
    }

    //
    // Collateral functions.
    //

    fun obtain_limit_order_collateral_internal<I, Q>(
        owner: &signer,
        side: u8,
        price: FixedPoint64,
        qty: FixedPoint64,
    ): (coin::Coin<Q>, coin::Coin<I>) {
        if (side == SIDE_BUY) {
            (
                coin::withdraw<Q>(
                    owner,
                    fixed_point_64::to_u64(fixed_point_64::multiply_round_up(price, qty), coin::decimals<Q>()),
                ),
                coin::zero<I>(),
            )
        } else {
            (
                coin::zero<Q>(),
                coin::withdraw<I>(
                    owner,
                    fixed_point_64::to_u64(qty, coin::decimals<I>()),
                ),
            )
        }
    }

    fun obtain_market_order_collateral_internal<I, Q>(
        owner: &signer,
        side: u8,
        qty: FixedPoint64,
        maxCollateralAmt: u64,
    ): (coin::Coin<Q>, coin::Coin<I>) {
        if (side == SIDE_BUY) {
            (coin::withdraw<Q>(owner, maxCollateralAmt), coin::zero<I>())
        } else {
            let coinDecimals = coin::decimals<I>();
            (coin::zero<Q>(), coin::withdraw<I>(owner,  fixed_point_64::to_u64(qty, coinDecimals)))
        }
    }

    // Moves collateral from orders to owners based on the execution details.
    fun swap_collateral<I, Q>(
        orderMap: &mut table::Table<OrderID, Order<I, Q>>,
        buyID: OrderID,
        sellID: OrderID,
        price: FixedPoint64,
        qty: FixedPoint64,
    ) {
        {
            let sellOwner = sellID.owner;
            let buy = table::borrow_mut(orderMap, buyID);
            let buyCollateral = extract_buy_collateral(buy, price, qty);
            coin::deposit(sellOwner, buyCollateral);
        };

        {
            let buyOwner = buyID.owner;
            let sell = table::borrow_mut(orderMap, sellID);
            let sellCollateral = extract_sell_collateral(sell, qty);
            coin::deposit(buyOwner, sellCollateral);
        };
    }

    fun extract_buy_collateral<I, Q>(order: &mut Order<I, Q>, price: FixedPoint64, qty: FixedPoint64): coin::Coin<Q> {
        let collateralUsed = fixed_point_64::multiply_trunc(price, qty);
        let coinDecimals = coin::decimals<Q>();
        let amt = fixed_point_64::to_u64(collateralUsed, coinDecimals);
        coin::extract(&mut order.buyCollateral, amt)
    }

    fun extract_sell_collateral<I, Q>(order: &mut Order<I, Q>, qty: FixedPoint64): coin::Coin<I> {
        let coinDecimals = coin::decimals<I>();
        let amt = fixed_point_64::to_u64(qty, coinDecimals);
        coin::extract(&mut order.sellCollateral, amt)
    }

    fun create_user_info_if_needed<I, Q>(owner: &signer) {
        if (exists<UserMarketInfo<I, Q>>(address_of(owner))) {
            return
        };
        move_to(owner, UserMarketInfo<I, Q> {
            idCounter: 0,
        });
    }

    #[test(owner = @ferum)]
    #[expected_failure]
    fun test_init_market_with_duplicate_market<I, Q>(owner: &signer) {
        init_ferum(owner);
        init_market_entry<I, Q>(owner, 4, 4);
        init_market_entry<I, Q>(owner, 4, 4);
    }

    #[test(owner = @ferum, user = @0x2)]
    #[expected_failure]
    fun test_add_limit_order_to_uninited_book(owner: &signer, user: &signer) acquires OrderBook, UserMarketInfo {
        // Tests that a limit order added for uninitialized book fails.

        account::create_account_for_test(address_of(owner));
        account::create_account_for_test(address_of(user));
        init_ferum(owner);
        setup_fake_coins(owner, user, 100, 18);
        add_limit_order_entry<FMA, FMB>(owner, SIDE_SELL, 1, 1, empty_client_order_id());
    }

    #[test(owner = @ferum, user = @0x2)]
    #[expected_failure]
    fun test_add_market_order_to_uninited_book(owner: &signer, user: &signer) acquires OrderBook, UserMarketInfo {
        // Tests that a limit order added for uninitialized book fails.

        account::create_account_for_test(address_of(owner));
        account::create_account_for_test(address_of(user));
        init_ferum(owner);
        setup_fake_coins(owner, user, 100, 18);
        add_market_order_entry<FMA, FMB>(owner, SIDE_SELL, 1, 1, empty_client_order_id());
    }

    #[test(owner = @ferum, aptos = @0x1, user = @0x2)]
    #[expected_failure]
    fun test_add_buy_order_exceed_balance(owner: &signer, aptos: &signer, user: &signer) acquires OrderBook, UserMarketInfo {
        // Tests that a buy order that requires more collateral than the user has fails.

        account::create_account_for_test(address_of(owner));
        account::create_account_for_test(address_of(user));
        setup_fake_coins(owner, user, 10000000000, 8); // Users have 100 FMA and FMB.
        setup_market_for_test<FMA, FMB>(owner, aptos);

        add_limit_order_entry<FMA, FMB>(owner, SIDE_BUY, 10000, 1200000, empty_client_order_id()); // BUY 120 FMA @ 1 FMB
    }

    #[test(owner = @ferum, aptos = @0x1, user = @0x2)]
    #[expected_failure]
    fun test_add_buy_order_exceed_balance_price(owner: &signer, aptos: &signer, user: &signer) acquires OrderBook, UserMarketInfo {
        // Tests that a buy order that requires more collateral than the user has fails.

        account::create_account_for_test(address_of(owner));
        account::create_account_for_test(address_of(user));
        setup_fake_coins(owner, user, 10000000000, 8); // Users have 100 FMA and FMB.
        setup_market_for_test<FMA, FMB>(owner, aptos);

        add_limit_order_entry<FMA, FMB>(owner, SIDE_BUY, 1200000, 10000, empty_client_order_id()); // BUY 1 FMA @ 120 FMB
    }

    #[test(owner = @ferum, aptos = @0x1, user = @0x2)]
    #[expected_failure]
    fun test_add_sell_order_exceed_balance(owner: &signer, aptos: &signer, user: &signer) acquires OrderBook, UserMarketInfo {
        // Tests that a sell order that requires more collateral than the user has fails.

        account::create_account_for_test(address_of(owner));
        account::create_account_for_test(address_of(user));
        setup_fake_coins(owner, user, 10000000000, 8); // Users have 100 FMA and FMB.
        setup_market_for_test<FMA, FMB>(owner, aptos);

        add_limit_order_entry<FMA, FMB>(owner, SIDE_SELL, 10000, 1200000, empty_client_order_id()); // SELL 120 FMA @ 1 FMB
    }

    #[test(owner = @ferum, aptos = @0x1, user = @0x2)]
    fun test_add_sell_order_no_precision_loss(owner: &signer, aptos: &signer, user: &signer) acquires OrderBook, UserMarketInfo {
        // Tests that a sell order placed with the minimum qty doesn't fail.

        account::create_account_for_test(address_of(owner));
        account::create_account_for_test(address_of(user));
        setup_fake_coins(owner, user, 10000000000, 8); // Users have 100 FMA and FMB.
        setup_market_for_test<FMA, FMB>(owner, aptos);

        // SELL 0.00000001 FMA @ 0.00000001 FMB
        // Requires obtaining 0.00000001 FMA of collateral, which is possible.
        add_limit_order_entry<FMA, FMB>(owner, SIDE_SELL, 1, 1, empty_client_order_id());
    }

    #[test(owner = @ferum, aptos = @0x1, user = @0x2)]
    fun test_add_orders_to_empty_book(owner: &signer, aptos: &signer, user: &signer) acquires OrderBook, UserMarketInfo {
        // Tests that orders can be added to empty book and none of them trigger.

        account::create_account_for_test(address_of(owner));
        account::create_account_for_test(address_of(user));
        setup_fake_coins(owner, user, 10000000000, 8);
        setup_market_for_test<FMA, FMB>(owner, aptos);

        add_limit_order_entry<FMA, FMB>(owner, SIDE_BUY, 10000, 100000, empty_client_order_id()); // BUY 10 FMA @ 1 FMB
        add_limit_order_entry<FMA, FMB>(owner, SIDE_BUY, 20000, 10000, empty_client_order_id()); // BUY 1 FMA @ 2 FMB
        add_limit_order_entry<FMA, FMB>(owner, SIDE_BUY, 100000, 10000, empty_client_order_id()); // BUY 1 FMA @ 10 FMB

        add_limit_order_entry<FMA, FMB>(owner, SIDE_SELL, 200000, 100000, empty_client_order_id()); // SELL 10 FMA @ 20 FMB
        add_limit_order_entry<FMA, FMB>(owner, SIDE_SELL, 210000, 10000, empty_client_order_id()); // SELL 1 FMA @ 21 FMB
        add_limit_order_entry<FMA, FMB>(owner, SIDE_SELL, 250000, 10000, empty_client_order_id()); // SELL 1 FMA @ 25 FMB

        assert!(coin::balance<FMA>(address_of(owner)) == 8800000000, 0);
        assert!(coin::balance<FMB>(address_of(owner)) == 7800000000, 0);
    }

    #[test(owner = @ferum, aptos = @0x1, user = @0x2)]
    fun test_cancel_orders(owner: &signer, aptos: &signer, user: &signer) acquires OrderBook, UserMarketInfo {
        // Tests that orders can be added a book and then cancelled.

        account::create_account_for_test(address_of(owner));
        account::create_account_for_test(address_of(user));
        setup_fake_coins(owner, user, 10000000000, 8);
        setup_market_for_test<FMA, FMB>(owner, aptos);

        // BUY 10 FMA @ 1 FMB
        let buyID = add_limit_order<FMA, FMB>(
            user,
            SIDE_BUY,
            from_u64(10000, 4),
            from_u64(100000, 4),
            empty_client_order_id(),
        );
        // SELL 10 FMA @ 20 FMB
        let sellID = add_limit_order<FMA, FMB>(
            user,
            SIDE_SELL,
            from_u64(200000, 4),
            from_u64(100000, 4),
            empty_client_order_id(),
        );

        cancel_order_entry<FMA, FMB>(user, buyID.counter);
        {
            let book = borrow_global<OrderBook<FMA, FMB>>(get_market_addr<FMA, FMB>());
            let order = get_order<FMA, FMB>(book, buyID);
            assert!(order.metadata.status == STATUS_CANCELLED, 0);
            assert!(coin::value(&order.buyCollateral) == 0, 0);
            assert!(coin::value(&order.sellCollateral) == 0, 0);
        };

        cancel_order_entry<FMA, FMB>(user, sellID.counter);
        {
            let book = borrow_global<OrderBook<FMA, FMB>>(get_market_addr<FMA, FMB>());
            let order = get_order<FMA, FMB>(book, sellID);
            assert!(order.metadata.status == STATUS_CANCELLED, 0);
            assert!(coin::value(&order.buyCollateral) == 0, 0);
            assert!(coin::value(&order.sellCollateral) == 0, 0);
        };

        assert!(coin::balance<FMB>(address_of(user)) == 10000000000, 0);
        assert!(coin::balance<FMA>(address_of(user)) == 10000000000, 0);
    }

    #[test(owner = @ferum, aptos = @0x1, user = @0x2, custodian = @0x4)]
    fun test_cancel_orders_as_custodian(owner: &signer, aptos: &signer, user: &signer, custodian: &signer) acquires OrderBook, UserMarketInfo {
        // Tests that custodial orders can be added a book and then cancelled.

        account::create_account_for_test(address_of(owner));
        account::create_account_for_test(address_of(user));
        setup_fake_coins(owner, user, 10000000000, 8);
        setup_market_for_test<FMA, FMB>(owner, aptos);

        let cap = register_custodian(custodian);

        // BUY 10 FMA @ 1 FMB
        let buyID = add_limit_order_as_custodian<FMA, FMB>(
            user,
            &cap,
            SIDE_BUY,
            from_u64(10000, 4),
            from_u64(100000, 4),
            empty_client_order_id(),
        );

        cancel_order_as_custodian<FMA, FMB>(custodian, &cap, address_of(user), buyID.counter);
        {
            let book = borrow_global<OrderBook<FMA, FMB>>(get_market_addr<FMA, FMB>());
            let order = get_order<FMA, FMB>(book, buyID);
            assert!(order.metadata.status == STATUS_CANCELLED, 0);
            assert!(coin::value(&order.buyCollateral) == 0, 0);
            assert!(coin::value(&order.sellCollateral) == 0, 0);
        };

        assert!(coin::balance<FMB>(address_of(user)) == 10000000000, 0);
        assert!(coin::balance<FMA>(address_of(user)) == 10000000000, 0);

        drop_custodian_capability(cap);
    }

    #[test(owner = @ferum, aptos = @0x1, user = @0x2, custodian = @0x4)]
    #[expected_failure]
    fun test_cancel_custodial_order_without_capability(owner: &signer, aptos: &signer, user: &signer, custodian: &signer) acquires OrderBook, UserMarketInfo {
        // Tests that custodial orders can't be cancelled by the user.

        account::create_account_for_test(address_of(owner));
        account::create_account_for_test(address_of(user));
        setup_fake_coins(owner, user, 10000000000, 8);
        setup_market_for_test<FMA, FMB>(owner, aptos);

        let cap = register_custodian(custodian);

        // BUY 10 FMA @ 1 FMB
        let buyID = add_limit_order_as_custodian<FMA, FMB>(
            user,
            &cap,
            SIDE_BUY,
            from_u64(10000, 4),
            from_u64(100000, 4),
            empty_client_order_id(),
        );

        cancel_order_entry<FMA, FMB>(user,  buyID.counter);

        drop_custodian_capability(cap);
    }

    #[test(owner = @ferum, aptos = @0x1, user = @0x2, custodian = @0x4)]
    #[expected_failure]
    fun test_cancel_non_custodial_order_as_custodian(owner: &signer, aptos: &signer, user: &signer, custodian: &signer) acquires OrderBook, UserMarketInfo {
        // Tests that non custodial orders can only be cancelled by the original user.

        account::create_account_for_test(address_of(owner));
        account::create_account_for_test(address_of(user));
        setup_fake_coins(owner, user, 10000000000, 8);
        setup_market_for_test<FMA, FMB>(owner, aptos);

        let cap = register_custodian(custodian);

        // BUY 10 FMA @ 1 FMB
        let buyID = add_limit_order<FMA, FMB>(
            user,
            SIDE_BUY,
            from_u64(10000, 4),
            from_u64(100000, 4),
            empty_client_order_id(),
        );

        cancel_order_as_custodian<FMA, FMB>(custodian, &cap, address_of(user), buyID.counter);

        drop_custodian_capability(cap);
    }

    #[test(owner = @ferum, aptos = @0x1, user1 = @0x2, user2 = @0x4)]
    #[expected_failure]
    fun test_cancel_order_wrong_user(owner: &signer, aptos: &signer, user1: &signer, user2: &signer) acquires OrderBook, UserMarketInfo {
        // Tests that non custodial orders can only be cancelled by the original user.

        account::create_account_for_test(address_of(owner));
        account::create_account_for_test(address_of(user1));
        setup_fake_coins(owner, user1, 10000000000, 8);
        setup_market_for_test<FMA, FMB>(owner, aptos);

        // BUY 10 FMA @ 1 FMB
        let buyID = add_limit_order<FMA, FMB>(
            user1,
            SIDE_BUY,
            from_u64(10000, 4),
            from_u64(100000, 4),
            empty_client_order_id(),
        );

        cancel_order_entry<FMA, FMB>(user2, buyID.counter);

    }

    #[test(owner = @ferum, aptos = @0x1, user = @0x2)]
    fun test_add_market_orders_cancelled(owner: &signer, aptos: &signer, user: &signer) acquires OrderBook, UserMarketInfo {
        // Tests that market orders should get cancelled because there is nothing to execute them against.

        account::create_account_for_test(address_of(owner));
        account::create_account_for_test(address_of(user));
        setup_fake_coins(owner, user, 10000000000, 8);
        setup_market_for_test<FMA, FMB>(owner, aptos);

        // BUY 1 FMA spending at most 10 FMB
        {
            let orderID = add_market_order<FMA, FMB>(
                owner,
                SIDE_BUY,
                from_u64(10000, 4),
                100000,
                empty_client_order_id(),
            );
            let book = borrow_global<OrderBook<FMA, FMB>>(get_market_addr<FMA, FMB>());
            let order = get_order<FMA, FMB>(book, orderID);
            assert!(order.metadata.status == STATUS_CANCELLED, 0);
            assert!(coin::value(&order.buyCollateral) == 0, 0);
            assert!(coin::value(&order.sellCollateral) == 0, 0);
        };

        // SELL 1 FMA.
        {
            let orderID = add_market_order<FMA, FMB>(
                owner,
                SIDE_SELL,
                from_u64(10000, 4),
                0,
                empty_client_order_id(),
            );
            let book = borrow_global<OrderBook<FMA, FMB>>(get_market_addr<FMA, FMB>());
            let order = get_order<FMA, FMB>(book, orderID);
            assert!(order.metadata.status == STATUS_CANCELLED, 0);
            assert!(coin::value(&order.buyCollateral) == 0, 0);
            assert!(coin::value(&order.sellCollateral) == 0, 0);
        };
    }

    #[test(owner = @ferum, aptos = @0x1, user1 = @0x2, user2 = @0x3)]
    fun test_market_buy_execute_against_limit(owner: &signer, aptos: &signer, user1: &signer, user2: &signer) acquires OrderBook, UserMarketInfo {
        // Tests that market buy order execute against limit orders.

        account::create_account_for_test(address_of(owner));
        account::create_account_for_test(address_of(user1));
        account::create_account_for_test(address_of(user2));
        setup_fake_coins(owner, user1, 10000000000, 8);
        register_fma(owner, user2, 10000000000);
        register_fmb(owner, user2, 10000000000);
        setup_market_for_test<FMA, FMB>(owner, aptos);

        // Book setup.
        add_limit_order_entry<FMA, FMB>(owner, SIDE_BUY, 10000, 100000, empty_client_order_id(), ); // BUY 10 FMA @ 1 FMB
        add_limit_order_entry<FMA, FMB>(owner, SIDE_BUY, 20000, 10000, empty_client_order_id(), ); // BUY 1 FMA @ 2 FMB
        add_limit_order_entry<FMA, FMB>(owner, SIDE_BUY, 100000, 10000, empty_client_order_id(), ); // BUY 1 FMA @ 10 FMB

        let targetSellID = add_limit_order<FMA, FMB>(  // SELL 10 FMA @ 20 FMB
            user2,
            SIDE_SELL,
            from_u64(200000, 4),
            from_u64(100000, 4),
            empty_client_order_id(),
        );
        add_limit_order_entry<FMA, FMB>(owner, SIDE_SELL, 210000, 10000, empty_client_order_id(), ); // SELL 1 FMA @ 21 FMB
        add_limit_order_entry<FMA, FMB>(owner, SIDE_SELL, 250000, 10000, empty_client_order_id(), );  // SELL 1 FMA @ 25 FMB

        // BUY 1 FMA spending at most 20 FMB.
        let orderID = add_market_order<FMA, FMB>(
            user1,
            SIDE_BUY,
            from_u64(10000, 4),
            2000000000,
            empty_client_order_id(),
        );

        // Verify market order user.
        {
            let book = borrow_global<OrderBook<FMA, FMB>>(get_market_addr<FMA, FMB>());
            let order = get_order<FMA, FMB>(book, orderID);
            assert!(order.metadata.status == STATUS_FILLED, 0);
            assert!(coin::value(&order.buyCollateral) == 0, 0);
            assert!(coin::value(&order.sellCollateral) == 0, 0);
            assert!(coin::balance<FMB>(address_of(user1)) == 8000000000, 0);
            assert!(coin::balance<FMA>(address_of(user1)) == 10100000000, 0);
        };

        // Verify limit order user.
        {
            let book = borrow_global<OrderBook<FMA, FMB>>(get_market_addr<FMA, FMB>());
            let order = get_order<FMA, FMB>(book, targetSellID);
            assert!(order.metadata.status == STATUS_PENDING, 0);
            assert!(coin::value(&order.buyCollateral) == 0, 0);
            assert!(coin::value(&order.sellCollateral) == 900000000, 0);
            assert!(coin::balance<FMB>(address_of(user2)) == 12000000000, 0);
            assert!(coin::balance<FMA>(address_of(user2)) == 9000000000, 0);
        };
    }

    #[test(owner = @ferum, aptos = @0x1, user1 = @0x2, user2 = @0x3)]
    fun test_limit_buy_execute_fully_against_limit(owner: &signer, aptos: &signer, user1: &signer, user2: &signer) acquires OrderBook, UserMarketInfo {
        // Tests that limit buy order executes completely against limit sell order.

        account::create_account_for_test(address_of(owner));
        account::create_account_for_test(address_of(user1));
        account::create_account_for_test(address_of(user2));
        setup_fake_coins(owner, user1, 20000000000, 8);
        register_fma(owner, user2, 20000000000);
        register_fmb(owner, user2, 20000000000);
        setup_market_for_test<FMA, FMB>(owner, aptos);

        // Book setup.
        add_limit_order_entry<FMA, FMB>(owner, SIDE_BUY, 10000, 100000, empty_client_order_id()); // BUY 10 FMA @ 1 FMB
        add_limit_order_entry<FMA, FMB>(owner, SIDE_BUY, 20000, 10000, empty_client_order_id()); // BUY 1 FMA @ 2 FMB
        add_limit_order_entry<FMA, FMB>(owner, SIDE_BUY, 100000, 10000, empty_client_order_id()); // BUY 1 FMA @ 10 FMB

        let targetSellID = add_limit_order<FMA, FMB>( // SELL 10 FMA @ 20 FMB
            user2,
            SIDE_SELL,
            from_u64(200000, 4),
            from_u64(100000, 4),
            empty_client_order_id(),
        );
        add_limit_order_entry<FMA, FMB>(owner, SIDE_SELL, 210000, 10000, empty_client_order_id()); // SELL 1 FMA @ 21 FMB
        add_limit_order_entry<FMA, FMB>(owner, SIDE_SELL, 250000, 10000, empty_client_order_id()); // SELL 1 FMA @ 25 FMB

        // BUY 10 FMA at 20 FMB.
        let orderID = add_limit_order<FMA, FMB>(
            user1,
            SIDE_BUY,
            from_u64(200000, 4),
            from_u64(100000, 4),
            empty_client_order_id(),
        );

        // Verify user1.
        {
            let book = borrow_global<OrderBook<FMA, FMB>>(get_market_addr<FMA, FMB>());
            let order = get_order<FMA, FMB>(book, orderID);
            assert!(order.metadata.status == STATUS_FILLED, 0);
            assert!(coin::value(&order.buyCollateral) == 0, 0);
            assert!(coin::value(&order.sellCollateral) == 0, 0);
            assert!(coin::balance<FMB>(address_of(user1)) == 0, 0);
            assert!(coin::balance<FMA>(address_of(user1)) == 21000000000, 0);
        };

        // Verify user2.
        {
            let book = borrow_global<OrderBook<FMA, FMB>>(get_market_addr<FMA, FMB>());
            let order = get_order<FMA, FMB>(book, targetSellID);
            assert!(order.metadata.status == STATUS_FILLED, 0);
            assert!(coin::value(&order.buyCollateral) == 0, 0);
            assert!(coin::value(&order.sellCollateral) == 0, 0);
            assert!(coin::balance<FMB>(address_of(user2)) == 40000000000, 0);
            assert!(coin::balance<FMA>(address_of(user2)) == 19000000000, 0);
        };
    }

    #[test(owner = @ferum, aptos = @0x1, user1 = @0x2, user2 = @0x3)]
    fun test_market_buy_execute_fully_against_limit(owner: &signer, aptos: &signer, user1: &signer, user2: &signer) acquires OrderBook, UserMarketInfo {
        // Tests that market buy order executes completely against limit sell order.

        account::create_account_for_test(address_of(owner));
        account::create_account_for_test(address_of(user1));
        account::create_account_for_test(address_of(user2));
        setup_fake_coins(owner, user1, 20000000000, 8);
        register_fma(owner, user2, 20000000000);
        register_fmb(owner, user2, 20000000000);
        setup_market_for_test<FMA, FMB>(owner, aptos);

        // Book setup.
        add_limit_order_entry<FMA, FMB>(owner, SIDE_BUY, 10000, 100000, empty_client_order_id()); // BUY 10 FMA @ 1 FMB
        add_limit_order_entry<FMA, FMB>(owner, SIDE_BUY, 20000, 10000, empty_client_order_id()); // BUY 1 FMA @ 2 FMB
        add_limit_order_entry<FMA, FMB>(owner, SIDE_BUY, 100000, 10000, empty_client_order_id()); // BUY 1 FMA @ 10 FMB

        let targetSellID = add_limit_order<FMA, FMB>(  // SELL 10 FMA @ 20 FMB
            user2,
            SIDE_SELL,
            from_u64(200000, 4),
            from_u64(100000, 4),
            empty_client_order_id(),
        );
        add_limit_order_entry<FMA, FMB>(owner, SIDE_SELL, 210000, 10000, empty_client_order_id()); // SELL 1 FMA @ 21 FMB
        add_limit_order_entry<FMA, FMB>(owner, SIDE_SELL, 250000, 10000, empty_client_order_id()); // SELL 1 FMA @ 25 FMB

        // BUY 10 FMA for at most 200 FMB.
        let orderID = add_market_order<FMA, FMB>(
            user1,
            SIDE_BUY,
            from_u64(100000, 4),
            20000000000,
            empty_client_order_id(),
        );

        // Verify market order user.
        {
            let book = borrow_global<OrderBook<FMA, FMB>>(get_market_addr<FMA, FMB>());
            let order = get_order<FMA, FMB>(book, orderID);
            assert!(order.metadata.status == STATUS_FILLED, 0);
            assert!(coin::value(&order.buyCollateral) == 0, 0);
            assert!(coin::value(&order.sellCollateral) == 0, 0);
            assert!(coin::balance<FMB>(address_of(user1)) == 0, 0);
            assert!(coin::balance<FMA>(address_of(user1)) == 21000000000, 0);
        };

        // Verify limit order user.
        {
            let book = borrow_global<OrderBook<FMA, FMB>>(get_market_addr<FMA, FMB>());
            let order = get_order<FMA, FMB>(book, targetSellID);
            assert!(order.metadata.status == STATUS_FILLED, 0);
            assert!(coin::value(&order.buyCollateral) == 0, 0);
            assert!(coin::value(&order.sellCollateral) == 0, 0);
            assert!(coin::balance<FMB>(address_of(user2)) == 40000000000, 0);
            assert!(coin::balance<FMA>(address_of(user2)) == 19000000000, 0);
        };
    }

    #[test(owner = @ferum, aptos = @0x1, user1 = @0x2, user2 = @0x3)]
    fun test_market_sell_execute_against_limit(owner: &signer, aptos: &signer, user1: &signer, user2: &signer) acquires OrderBook, UserMarketInfo {
        // Tests that market sell order execute against limit orders.

        account::create_account_for_test(address_of(owner));
        account::create_account_for_test(address_of(user1));
        account::create_account_for_test(address_of(user2));
        setup_fake_coins(owner, user1, 10000000000, 8);
        register_fma(owner, user2, 10000000000);
        register_fmb(owner, user2, 10000000000);
        setup_market_for_test<FMA, FMB>(owner, aptos);

        // Book setup.
        add_limit_order_entry<FMA, FMB>(owner, SIDE_BUY, 10000, 100000, empty_client_order_id()); // BUY 10 FMA @ 1 FMB
        add_limit_order_entry<FMA, FMB>(owner, SIDE_BUY, 20000, 10000, empty_client_order_id()); // BUY 1 FMA @ 2 FMB
        let targetBuyID = add_limit_order<FMA, FMB>( // BUY 1 FMA @ 10 FMB
            user2,
            SIDE_BUY,
            from_u64(100000, 4),
            from_u64(10000, 4),
            empty_client_order_id(),
        );

        add_limit_order_entry<FMA, FMB>(owner,SIDE_SELL,200000,100000,empty_client_order_id());  // SELL 10 FMA @ 20 FMB
        add_limit_order_entry<FMA, FMB>(owner, SIDE_SELL, 210000, 10000, empty_client_order_id()); // SELL 1 FMA @ 21 FMB
        add_limit_order_entry<FMA, FMB>(owner, SIDE_SELL, 250000, 10000, empty_client_order_id()); // SELL 1 FMA @ 25 FMB

        // SELL 1 FMA.
        let orderID = add_market_order<FMA, FMB>(
            user1,
            SIDE_SELL,
            from_u64(10000, 4),
            0,
            empty_client_order_id(),
        );

        // Verify market order user.
        {
            let book = borrow_global<OrderBook<FMA, FMB>>(get_market_addr<FMA, FMB>());
            let order = get_order<FMA, FMB>(book, orderID);
            assert!(order.metadata.status == STATUS_FILLED, 0);
            assert!(coin::value(&order.buyCollateral) == 0, 0);
            assert!(coin::value(&order.sellCollateral) == 0, 0);
            assert!(coin::balance<FMB>(address_of(user1)) == 11000000000, 0);
            assert!(coin::balance<FMA>(address_of(user1)) == 9900000000, 0);
        };

        // Verify limit order user.
        {
            let book = borrow_global<OrderBook<FMA, FMB>>(get_market_addr<FMA, FMB>());
            let order = get_order<FMA, FMB>(book, targetBuyID);
            assert!(order.metadata.status == STATUS_FILLED, 0);
            assert!(coin::value(&order.buyCollateral) == 0, 0);
            assert!(coin::value(&order.sellCollateral) == 0, 0);
            assert!(coin::balance<FMB>(address_of(user2)) == 9000000000, 0);
            assert!(coin::balance<FMA>(address_of(user2)) == 10100000000, 0);
        };
    }

    #[test(owner = @ferum, aptos = @0x1, user1 = @0x2, user2 = @0x3)]
    fun test_market_sell_execute_against_multiple_limits(owner: &signer, aptos: &signer, user1: &signer, user2: &signer) acquires OrderBook, UserMarketInfo {
        // Tests that market sell order execute against multiple limit orders.

        account::create_account_for_test(address_of(owner));
        account::create_account_for_test(address_of(user1));
        account::create_account_for_test(address_of(user2));
        setup_fake_coins(owner, user1, 10000000000, 8);
        register_fma(owner, user2, 10000000000);
        register_fmb(owner, user2, 10000000000);
        setup_market_for_test<FMA, FMB>(owner, aptos);

        // Book setup.
        let targetBuyIDC = add_limit_order<FMA, FMB>( // BUY 10 FMA @ 1 FMB
            user2,
            SIDE_BUY,
            from_u64(10000, 4),
            from_u64(100000, 4),
            empty_client_order_id(),
        );
        let targetBuyIDB = add_limit_order<FMA, FMB>( // BUY 1 FMA @ 2 FMB
            user2,
            SIDE_BUY,
            from_u64(20000, 4),
            from_u64(10000, 4),
            empty_client_order_id(),
        );
        let targetBuyIDA = add_limit_order<FMA, FMB>( // BUY 1 FMA @ 10 FMB
            user2,
            SIDE_BUY,
            from_u64(100000, 4),
            from_u64(10000, 4),
            empty_client_order_id(),
        );

        add_limit_order_entry<FMA, FMB>(owner, SIDE_SELL, 200000, 100000, empty_client_order_id());  // SELL 10 FMA @ 20 FMB
        add_limit_order_entry<FMA, FMB>(owner, SIDE_SELL, 210000, 10000, empty_client_order_id()); // SELL 1 FMA @ 21 FMB
        add_limit_order_entry<FMA, FMB>(owner, SIDE_SELL, 250000, 10000, empty_client_order_id()); // SELL 1 FMA @ 25 FMB

        // SELL 5 FMA.
        let orderID = add_market_order<FMA, FMB>(
            user1,
            SIDE_SELL,
            from_u64(50000, 4),
            0,
            empty_client_order_id(),
        );

        // Verify market order user.
        {
            let book = borrow_global<OrderBook<FMA, FMB>>(get_market_addr<FMA, FMB>());
            let order = get_order<FMA, FMB>(book, orderID);

            assert!(order.metadata.status == STATUS_FILLED, 0);
            assert!(coin::value(&order.buyCollateral) == 0, 0);
            assert!(coin::value(&order.sellCollateral) == 0, 0);
            assert!(coin::balance<FMB>(address_of(user1)) == 11500000000, 0);
            assert!(coin::balance<FMA>(address_of(user1)) == 9500000000, 0);
        };

        // Verify limit order user.
        {
            let book = borrow_global<OrderBook<FMA, FMB>>(get_market_addr<FMA, FMB>());

            let orderA = get_order<FMA, FMB>(book, targetBuyIDA);
            assert!(orderA.metadata.status == STATUS_FILLED, 0);
            assert!(coin::value(&orderA.buyCollateral) == 0, 0);
            assert!(coin::value(&orderA.sellCollateral) == 0, 0);

            let orderB = get_order<FMA, FMB>(book, targetBuyIDB);
            assert!(orderB.metadata.status == STATUS_FILLED, 0);
            assert!(coin::value(&orderB.buyCollateral) == 0, 0);
            assert!(coin::value(&orderB.sellCollateral) == 0, 0);

            let orderC = get_order<FMA, FMB>(book, targetBuyIDC);
            assert!(orderC.metadata.status == STATUS_PENDING, 0);
            assert!(coin::value(&orderC.buyCollateral) == 700000000, 0);
            assert!(coin::value(&orderC.sellCollateral) == 0, 0);

            assert!(coin::balance<FMB>(address_of(user2)) == 7800000000, 0);
            assert!(coin::balance<FMA>(address_of(user2)) == 10500000000, 0);
        };
    }

    #[test(owner = @ferum, aptos = @0x1, user1 = @0x2, user2 = @0x3)]
    fun test_market_buy_execute_against_multiple_limits(owner: &signer, aptos: &signer, user1: &signer, user2: &signer) acquires OrderBook, UserMarketInfo {
        // Tests that market buy order execute against multiple limit orders.

        account::create_account_for_test(address_of(owner));
        account::create_account_for_test(address_of(user1));
        account::create_account_for_test(address_of(user2));
        setup_fake_coins(owner, user1, 50000000000, 8);
        register_fma(owner, user2, 50000000000);
        register_fmb(owner, user2, 50000000000);
        setup_market_for_test<FMA, FMB>(owner, aptos);

        // Book setup.
        add_limit_order_entry<FMA, FMB>(owner, SIDE_BUY, 10000, 100000, empty_client_order_id()); // BUY 10 FMA @ 1 FMB
        add_limit_order_entry<FMA, FMB>(owner, SIDE_BUY, 20000, 10000, empty_client_order_id()); // BUY 1 FMA @ 2 FMB
        add_limit_order_entry<FMA, FMB>(owner, SIDE_BUY, 100000, 10000, empty_client_order_id()); // BUY 1 FMA @ 10 FMB

        let targetSellIDA = add_limit_order<FMA, FMB>(  // SELL 10 FMA @ 20 FMB
            user2,
            SIDE_SELL,
            from_u64(200000, 4),
            from_u64(100000, 4),
            empty_client_order_id(),
        );
        let targetSellIDB = add_limit_order<FMA, FMB>( // SELL 1 FMA @ 21 FMB
            user2,
            SIDE_SELL,
            from_u64(210000, 4),
            from_u64(10000, 4),
            empty_client_order_id(),
        );
        let targetSellIDC = add_limit_order<FMA, FMB>( // SELL 1 FMA @ 25 FMB
            user2,
            SIDE_SELL,
            from_u64(250000, 4),
            from_u64(10000, 4),
            empty_client_order_id(),
        );

        // BUY 12 FMA spending at most 360 FMB.
        let orderID = add_market_order<FMA, FMB>(
            user1,
            SIDE_BUY,
            from_u64(120000, 4),
            36000000000,
            empty_client_order_id(),
        );

        // Verify market order user.
        {
            let book = borrow_global<OrderBook<FMA, FMB>>(get_market_addr<FMA, FMB>());
            let order = get_order<FMA, FMB>(book, orderID);

            assert!(order.metadata.status == STATUS_FILLED, 0);
            assert!(coin::value(&order.buyCollateral) == 0, 0);
            assert!(coin::value(&order.sellCollateral) == 0, 0);
            assert!(coin::balance<FMB>(address_of(user1)) == 25400000000, 0);
            assert!(coin::balance<FMA>(address_of(user1)) == 51200000000, 0);
        };

        // Verify limit order user.
        {
            let book = borrow_global<OrderBook<FMA, FMB>>(get_market_addr<FMA, FMB>());

            let orderA = get_order<FMA, FMB>(book, targetSellIDA);
            assert!(orderA.metadata.status == STATUS_FILLED, 0);
            assert!(coin::value(&orderA.buyCollateral) == 0, 0);
            assert!(coin::value(&orderA.sellCollateral) == 0, 0);

            let orderB = get_order<FMA, FMB>(book, targetSellIDB);
            assert!(orderB.metadata.status == STATUS_FILLED, 0);
            assert!(coin::value(&orderB.buyCollateral) == 0, 0);
            assert!(coin::value(&orderB.sellCollateral) == 0, 0);

            let orderC = get_order<FMA, FMB>(book, targetSellIDC);
            assert!(orderC.metadata.status == STATUS_FILLED, 0);
            assert!(coin::value(&orderC.buyCollateral) == 0, 0);
            assert!(coin::value(&orderC.sellCollateral) == 0, 0);

            assert!(coin::balance<FMB>(address_of(user2)) == 74600000000, 0);
            assert!(coin::balance<FMA>(address_of(user2)) == 48800000000, 0);
        };
    }

    #[test(owner = @ferum, aptos = @0x1, user1 = @0x2, user2 = @0x3)]
    fun test_market_sell_eat_book_not_filled(owner: &signer, aptos: &signer, user1: &signer, user2: &signer) acquires OrderBook, UserMarketInfo {
        // Tests that market sell order that eats through the book is cancelled.

        account::create_account_for_test(address_of(owner));
        account::create_account_for_test(address_of(user1));
        account::create_account_for_test(address_of(user2));
        setup_fake_coins(owner, user1, 50000000000, 8);
        register_fma(owner, user2, 50000000000);
        register_fmb(owner, user2, 50000000000);
        setup_market_for_test<FMA, FMB>(owner, aptos);

        // Book setup.
        let targetBuyIDA = add_limit_order<FMA, FMB>( // BUY 1 FMA @ 10 FMB
            user2,
            SIDE_BUY,
            from_u64(100000, 4),
            from_u64(10000, 4),
            empty_client_order_id(),
        );

        add_limit_order_entry<FMA, FMB>(owner, SIDE_SELL, 200000, 100000, empty_client_order_id());  // SELL 10 FMA @ 20 FMB
        add_limit_order_entry<FMA, FMB>(owner, SIDE_SELL, 210000, 10000, empty_client_order_id()); // SELL 1 FMA @ 21 FMB
        add_limit_order_entry<FMA, FMB>(owner, SIDE_SELL, 250000, 10000, empty_client_order_id()); // SELL 1 FMA @ 25 FMB

        // SELL 2 FMA.
        let orderID = add_market_order<FMA, FMB>(
            user1,
            SIDE_SELL,
            from_u64(20000, 4),
            0,
            empty_client_order_id(),
        );

        // Verify market order user.
        {
            let book = borrow_global<OrderBook<FMA, FMB>>(get_market_addr<FMA, FMB>());
            let order = get_order<FMA, FMB>(book, orderID);

            assert!(order.metadata.status == STATUS_CANCELLED, 0);
            assert!(coin::value(&order.buyCollateral) == 0, 0);
            assert!(coin::value(&order.sellCollateral) == 0, 0);
            assert!(coin::balance<FMB>(address_of(user1)) == 51000000000, 0);
            assert!(coin::balance<FMA>(address_of(user1)) == 49900000000, 0);
        };

        // Verify limit order user.
        {
            let book = borrow_global<OrderBook<FMA, FMB>>(get_market_addr<FMA, FMB>());

            let orderA = get_order<FMA, FMB>(book, targetBuyIDA);
            assert!(orderA.metadata.status == STATUS_FILLED, 0);
            assert!(coin::value(&orderA.buyCollateral) == 0, 0);
            assert!(coin::value(&orderA.sellCollateral) == 0, 0);

            assert!(coin::balance<FMB>(address_of(user2)) == 49000000000, 0);
            assert!(coin::balance<FMA>(address_of(user2)) == 50100000000, 0);
        };
    }

    #[test(owner = @ferum, aptos = @0x1, user1 = @0x2, user2 = @0x3)]
    fun test_market_buy_eat_book_not_filled(owner: &signer, aptos: &signer, user1: &signer, user2: &signer) acquires OrderBook, UserMarketInfo {
        // Tests that market buy order that eats through the book is cancelled.

        account::create_account_for_test(address_of(owner));
        account::create_account_for_test(address_of(user1));
        account::create_account_for_test(address_of(user2));
        setup_fake_coins(owner, user1, 50000000000, 8);
        register_fma(owner, user2, 50000000000);
        register_fmb(owner, user2, 50000000000);
        setup_market_for_test<FMA, FMB>(owner, aptos);

        // Book setup.
        add_limit_order_entry<FMA, FMB>(owner, SIDE_BUY, 10000, 100000, empty_client_order_id()); // BUY 10 FMA @ 1 FMB
        add_limit_order_entry<FMA, FMB>(owner, SIDE_BUY, 20000, 10000, empty_client_order_id()); // BUY 1 FMA @ 2 FMB
        add_limit_order_entry<FMA, FMB>(owner, SIDE_BUY, 100000, 10000, empty_client_order_id()); // BUY 1 FMA @ 10 FMB

        let targetSellIDA = add_limit_order<FMA, FMB>( // SELL 1 FMA @ 25 FMB
            user2,
            SIDE_SELL,
            from_u64(250000, 4),
            from_u64(10000, 4),
            empty_client_order_id(),
        );

        // BUY 2 FMA spending at most 360 FMB.
        let orderID = add_market_order<FMA, FMB>(
            user1,
            SIDE_BUY,
            from_u64(20000, 4),
            36000000000,
            empty_client_order_id(),
        );

        // Verify market order user.
        {
            let book = borrow_global<OrderBook<FMA, FMB>>(get_market_addr<FMA, FMB>());
            let order = get_order<FMA, FMB>(book, orderID);

            assert!(order.metadata.status == STATUS_CANCELLED, 0);
            assert!(coin::value(&order.buyCollateral) == 0, 0);
            assert!(coin::value(&order.sellCollateral) == 0, 0);
            assert!(coin::balance<FMB>(address_of(user1)) == 47500000000, 0);
            assert!(coin::balance<FMA>(address_of(user1)) == 50100000000, 0);
        };

        // Verify limit order user.
        {
            let book = borrow_global<OrderBook<FMA, FMB>>(get_market_addr<FMA, FMB>());

            let orderA = get_order<FMA, FMB>(book, targetSellIDA);
            assert!(orderA.metadata.status == STATUS_FILLED, 0);
            assert!(coin::value(&orderA.buyCollateral) == 0, 0);
            assert!(coin::value(&orderA.sellCollateral) == 0, 0);

            assert!(coin::balance<FMB>(address_of(user2)) == 52500000000, 0);
            assert!(coin::balance<FMA>(address_of(user2)) == 49900000000, 0);
        };
    }

    #[test(owner = @ferum, aptos = @0x1, user1 = @0x2, user2 = @0x3)]
    fun test_limit_buy_execute(owner: &signer, aptos: &signer, user1: &signer, user2: &signer) acquires OrderBook, UserMarketInfo {
        // Tests that limit buy order executes against other limit orders.

        account::create_account_for_test(address_of(owner));
        account::create_account_for_test(address_of(user1));
        account::create_account_for_test(address_of(user2));
        setup_fake_coins(owner, user1, 10000000000, 8);
        register_fma(owner, user2, 10000000000);
        register_fmb(owner, user2, 10000000000);
        setup_market_for_test<FMA, FMB>(owner, aptos);

        // Book setup.
        add_limit_order_entry<FMA, FMB>(owner, SIDE_BUY, 10000, 100000, empty_client_order_id()); // BUY 10 FMA @ 1 FMB
        add_limit_order_entry<FMA, FMB>(owner, SIDE_BUY, 20000, 10000, empty_client_order_id()); // BUY 1 FMA @ 2 FMB
        add_limit_order_entry<FMA, FMB>(owner, SIDE_BUY, 100000, 10000, empty_client_order_id()); // BUY 1 FMA @ 10 FMB

        let targetSellID = add_limit_order<FMA, FMB>( // SELL 10 FMA @ 20 FMB
            user2,
            SIDE_SELL,
            from_u64(200000, 4),
            from_u64(100000, 4),
            empty_client_order_id(),
        );
        add_limit_order_entry<FMA, FMB>(owner, SIDE_SELL, 210000, 10000, empty_client_order_id()); // SELL 1 FMA @ 21 FMB
        add_limit_order_entry<FMA, FMB>(owner, SIDE_SELL, 250000, 10000, empty_client_order_id()); // SELL 1 FMA @ 25 FMB

        // BUY 1 FMA @ 20 FMB.
        let orderID = add_limit_order<FMA, FMB>(
            user1,
            SIDE_BUY,
            from_u64(200000, 4),
            from_u64(10000, 4),
            empty_client_order_id(),
        );

        // Verify buy order user.
        {
            let book = borrow_global<OrderBook<FMA, FMB>>(get_market_addr<FMA, FMB>());
            let order = get_order<FMA, FMB>(book, orderID);
            assert!(order.metadata.status == STATUS_FILLED, 0);
            assert!(coin::value(&order.buyCollateral) == 0, 0);
            assert!(coin::value(&order.sellCollateral) == 0, 0);
            assert!(coin::balance<FMB>(address_of(user1)) == 8000000000, 0);
            assert!(coin::balance<FMA>(address_of(user1)) == 10100000000, 0);
        };

        // Verify sell order user.
        {
            let book = borrow_global<OrderBook<FMA, FMB>>(get_market_addr<FMA, FMB>());
            let order = get_order<FMA, FMB>(book, targetSellID);
            assert!(order.metadata.status == STATUS_PENDING, 0);
            assert!(coin::value(&order.buyCollateral) == 0, 0);
            assert!(coin::value(&order.sellCollateral) == 900000000, 0);
            assert!(coin::balance<FMB>(address_of(user2)) == 12000000000, 0);
            assert!(coin::balance<FMA>(address_of(user2)) == 9000000000, 0);
        };
    }

    #[test(owner = @ferum, aptos = @0x1, user1 = @0x2, user2 = @0x3)]
    fun test_limit_sell_execute(owner: &signer, aptos: &signer, user1: &signer, user2: &signer) acquires OrderBook, UserMarketInfo {
        // Tests that limit sell order executes against other limit orders.

        account::create_account_for_test(address_of(owner));
        account::create_account_for_test(address_of(user1));
        account::create_account_for_test(address_of(user2));
        setup_fake_coins(owner, user1, 10000000000, 8);
        register_fma(owner, user2, 10000000000);
        register_fmb(owner, user2, 10000000000);
        setup_market_for_test<FMA, FMB>(owner, aptos);

        // Book setup.
        add_limit_order_entry<FMA, FMB>(owner, SIDE_BUY, 10000, 100000, empty_client_order_id()); // BUY 10 FMA @ 1 FMB
        add_limit_order_entry<FMA, FMB>(owner, SIDE_BUY, 95000, 10000, empty_client_order_id()); // BUY 1 FMA @ 9.5 FMB
        let targetBuyID = add_limit_order<FMA, FMB>( // BUY 1 FMA @ 10 FMB
            user2,
            SIDE_BUY,
            from_u64(100000, 4),
            from_u64(10000, 4),
            empty_client_order_id(),
        );

        add_limit_order_entry<FMA, FMB>(owner, SIDE_SELL, 200000, 100000, empty_client_order_id()); // SELL 10 FMA @ 20 FMB
        add_limit_order_entry<FMA, FMB>(owner, SIDE_SELL, 210000, 10000, empty_client_order_id()); // SELL 1 FMA @ 21 FMB
        add_limit_order_entry<FMA, FMB>(owner, SIDE_SELL, 250000, 10000, empty_client_order_id()); // SELL 1 FMA @ 25 FMB

        // SELL 1 FMA @ 9 FMB.
        let orderID = add_limit_order<FMA, FMB>(
            user1,
            SIDE_SELL,
            from_u64(90000, 4),
            from_u64(10000, 4),
            empty_client_order_id(),
        );

        // Verify sell order user.
        {
            let book = borrow_global<OrderBook<FMA, FMB>>(get_market_addr<FMA, FMB>());
            let order = get_order<FMA, FMB>(book, orderID);
            assert!(order.metadata.status == STATUS_FILLED, 0);
            assert!(coin::value(&order.buyCollateral) == 0, 0);
            assert!(coin::value(&order.sellCollateral) == 0, 0);
            assert!(coin::balance<FMA>(address_of(user1)) == 9900000000, 0);
            assert!(coin::balance<FMB>(address_of(user1)) == 10950000000, 0);
        };

        // Verify buy order user.
        {
            let book = borrow_global<OrderBook<FMA, FMB>>(get_market_addr<FMA, FMB>());
            let order = get_order<FMA, FMB>(book, targetBuyID);
            assert!(order.metadata.status == STATUS_FILLED, 0);
            assert!(coin::value(&order.buyCollateral) == 0, 0);
            assert!(coin::value(&order.sellCollateral) == 0, 0);
            assert!(coin::balance<FMB>(address_of(user2)) == 9050000000, 0);
            assert!(coin::balance<FMA>(address_of(user2)) == 10100000000, 0);
        };
    }

    #[test(owner = @ferum, aptos = @0x1, user1 = @0x2, user2 = @0x3)]
    fun test_limit_buy_execute_multiple(owner: &signer, aptos: &signer, user1: &signer, user2: &signer) acquires OrderBook, UserMarketInfo {
        // Tests that limit buy order executes against multiple other limit orders.

        account::create_account_for_test(address_of(owner));
        account::create_account_for_test(address_of(user1));
        account::create_account_for_test(address_of(user2));
        setup_fake_coins(owner, user1, 50000000000, 8);
        register_fma(owner, user2, 50000000000);
        register_fmb(owner, user2, 50000000000);
        setup_market_for_test<FMA, FMB>(owner, aptos);

        // Book setup.
        add_limit_order_entry<FMA, FMB>(owner, SIDE_BUY, 10000, 100000, empty_client_order_id()); // BUY 10 FMA @ 1 FMB
        add_limit_order_entry<FMA, FMB>(owner, SIDE_BUY, 20000, 10000, empty_client_order_id()); // BUY 1 FMA @ 2 FMB
        add_limit_order_entry<FMA, FMB>(owner, SIDE_BUY, 100000, 10000, empty_client_order_id()); // BUY 1 FMA @ 10 FMB

        let targetSellIDA = add_limit_order<FMA, FMB>( // SELL 10 FMA @ 20 FMB
            user2,
            SIDE_SELL,
            from_u64(200000, 4),
            from_u64(100000, 4),
            empty_client_order_id(),
        );
        let targetSellIDB = add_limit_order<FMA, FMB>( // SELL 1 FMA @ 21 FMB
            user2,
            SIDE_SELL,
            from_u64(210000, 4),
            from_u64(10000, 4),
            empty_client_order_id(),
        );
        let targetSellIDC = add_limit_order<FMA, FMB>( // SELL 1 FMA @ 25 FMB
            user2,
            SIDE_SELL,
            from_u64(250000, 4),
            from_u64(10000, 4),
            empty_client_order_id(),
        );

        // BUY 11 FMA @ 22 FMB.
        let orderID = add_limit_order<FMA, FMB>(
            user1,
            SIDE_BUY,
            from_u64(220000, 4),
            from_u64(110000, 4),
            empty_client_order_id(),
        );

        // Verify buy order user.
        {
            let book = borrow_global<OrderBook<FMA, FMB>>(get_market_addr<FMA, FMB>());
            let order = get_order<FMA, FMB>(book, orderID);
            assert!(order.metadata.status == STATUS_FILLED, 0);
            assert!(coin::value(&order.buyCollateral) == 0, 0);
            assert!(coin::value(&order.sellCollateral) == 0, 0);
            assert!(coin::balance<FMB>(address_of(user1)) == 26850000000, 0);
            assert!(coin::balance<FMA>(address_of(user1)) == 51100000000, 0);
        };

        // Verify sell orders' users.
        {
            let book = borrow_global<OrderBook<FMA, FMB>>(get_market_addr<FMA, FMB>());

            let orderA = get_order<FMA, FMB>(book, targetSellIDA);
            assert!(orderA.metadata.status == STATUS_FILLED, 0);
            assert!(coin::value(&orderA.buyCollateral) == 0, 0);
            assert!(coin::value(&orderA.sellCollateral) == 0, 0);

            let orderB = get_order<FMA, FMB>(book, targetSellIDB);
            assert!(orderB.metadata.status == STATUS_FILLED, 0);
            assert!(coin::value(&orderB.buyCollateral) == 0, 0);
            assert!(coin::value(&orderB.sellCollateral) == 0, 0);

            let orderC = get_order<FMA, FMB>(book, targetSellIDC);
            assert!(orderC.metadata.status == STATUS_PENDING, 0);
            assert!(coin::value(&orderC.buyCollateral) == 0, 0);
            assert!(coin::value(&orderC.sellCollateral) == 100000000, 0);

            assert!(coin::balance<FMB>(address_of(user2)) == 73150000000, 0);
            assert!(coin::balance<FMA>(address_of(user2)) == 48800000000, 0);
        };
    }

    #[test(owner = @ferum, aptos = @0x1, user1 = @0x2, user2 = @0x3)]
    fun test_limit_sell_execute_multiple(owner: &signer, aptos: &signer, user1: &signer, user2: &signer) acquires OrderBook, UserMarketInfo {
        // Tests that limit sell order executes against multiple other limit orders.

        account::create_account_for_test(address_of(owner));
        account::create_account_for_test(address_of(user1));
        account::create_account_for_test(address_of(user2));
        setup_fake_coins(owner, user1, 50000000000, 8);
        register_fma(owner, user2, 50000000000);
        register_fmb(owner, user2, 50000000000);
        setup_market_for_test<FMA, FMB>(owner, aptos);

        // Book setup.
        let targetBuyIDC = add_limit_order<FMA, FMB>( // BUY 10 FMA @ 1 FMB
            user2,
            SIDE_BUY,
            from_u64(10000, 4),
            from_u64(100000, 4),
            empty_client_order_id(),
        );
        let targetBuyIDB = add_limit_order<FMA, FMB>( // BUY 1 FMA @ 2 FMB
            user2,
            SIDE_BUY,
            from_u64(20000, 4),
            from_u64(10000, 4),
            empty_client_order_id(),
        );
        let targetBuyIDA = add_limit_order<FMA, FMB>( // BUY 1 FMA @ 10 FMB
            user2,
            SIDE_BUY,
            from_u64(100000, 4),
            from_u64(10000, 4),
            empty_client_order_id(),
        );

        add_limit_order_entry<FMA, FMB>(owner, SIDE_SELL, 200000, 100000, empty_client_order_id()); // SELL 10 FMA @ 20 FMB
        add_limit_order_entry<FMA, FMB>(owner, SIDE_SELL, 210000, 10000, empty_client_order_id()); // SELL 1 FMA @ 21 FMB
        add_limit_order_entry<FMA, FMB>(owner, SIDE_SELL, 250000, 10000, empty_client_order_id()); // SELL 1 FMA @ 25 FMB

        // SELL 11 FMA @ 1.5 FMB.
        let orderID = add_limit_order<FMA, FMB>(
            user1,
            SIDE_SELL,
            from_u64(15000, 4),
            from_u64(110000, 4),
            empty_client_order_id(),
        );

        // Verify sell order user.
        {
            let book = borrow_global<OrderBook<FMA, FMB>>(get_market_addr<FMA, FMB>());
            let order = get_order<FMA, FMB>(book, orderID);
            assert!(order.metadata.status == STATUS_PENDING, 0);
            assert!(coin::value(&order.buyCollateral) == 0, 0);
            assert!(coin::value(&order.sellCollateral) == 900000000, 0);
            assert!(coin::balance<FMB>(address_of(user1)) == 50750000000, 0);
            assert!(coin::balance<FMA>(address_of(user1)) == 48900000000, 0);
        };

        // Verify buy orders' user.
        {
            let book = borrow_global<OrderBook<FMA, FMB>>(get_market_addr<FMA, FMB>());

            let orderA = get_order<FMA, FMB>(book, targetBuyIDA);
            assert!(orderA.metadata.status == STATUS_FILLED, 0);
            assert!(coin::value(&orderA.buyCollateral) == 0, 0);
            assert!(coin::value(&orderA.sellCollateral) == 0, 0);

            let orderB = get_order<FMA, FMB>(book, targetBuyIDB);
            assert!(orderB.metadata.status == STATUS_FILLED, 0);
            assert!(coin::value(&orderB.buyCollateral) == 0, 0);
            assert!(coin::value(&orderB.sellCollateral) == 0, 0);

            let orderC = get_order<FMA, FMB>(book, targetBuyIDC);
            assert!(orderC.metadata.status == STATUS_PENDING, 0);
            assert!(coin::value(&orderC.buyCollateral) == 1000000000, 0);
            assert!(coin::value(&orderC.sellCollateral) == 0, 0);

            assert!(coin::balance<FMB>(address_of(user2)) == 48250000000, 0);
            assert!(coin::balance<FMA>(address_of(user2)) == 50200000000, 0);
        };
    }

    #[test(owner = @ferum, aptos = @0x1, user1 = @0x2, user2 = @0x3)]
    fun test_limit_orders_precision(owner: &signer, aptos: &signer, user1: &signer, user2: &signer) acquires OrderBook, UserMarketInfo {
        // Tests that limit order executions that require more precision than the market's instrumentDecimals or
        // quoteDecimals don't fail (because both parameters are set so that they can be multiplied without exceeding
        // the underlying coin's decimals).

        account::create_account_for_test(address_of(owner));
        account::create_account_for_test(address_of(user1));
        account::create_account_for_test(address_of(user2));
        setup_fake_coins(owner, user1, 50000000000, 8);
        register_fma(owner, user2, 50000000000);
        register_fmb(owner, user2, 50000000000);
        setup_market_for_test<FMA, FMB>(owner, aptos);

        // Book setup.
        let buyID = add_limit_order<FMA, FMB>( // BUY 0.0002 FMA @ 0.0002 FMB
            user2,
            SIDE_BUY,
            from_u64(2, 4),
            from_u64(2, 4),
            empty_client_order_id(),
        );
        let sellID = add_limit_order<FMA, FMB>( // SELL 0.0001 FMA @ 0.0001 FMB
            user1,
            SIDE_SELL,
            from_u64(1, 4),
            from_u64(1, 4),
            empty_client_order_id(),
        );

        // Note that the midpoint here does exceed the max allowed precision of the underlying quote coin but
        // we round up:
        //
        // price = 0.00015
        // qty = 0.0001
        // buy cost = 0.000000015 -> rounded up = 0.00000002

        // Verify buy.
        {
            let book = borrow_global<OrderBook<FMA, FMB>>(get_market_addr<FMA, FMB>());
            let order = get_order<FMA, FMB>(book, buyID);
            assert!(order.metadata.status == STATUS_PENDING, 0);
            assert!(coin::value(&order.buyCollateral) == 2, 0);
            assert!(coin::value(&order.sellCollateral) == 0, 0);
            assert!(coin::balance<FMB>(address_of(user2)) == 49999999996, 0);
            assert!(coin::balance<FMA>(address_of(user2)) == 50000010000, 0);
        };

        // Verify sell.
        {
            let book = borrow_global<OrderBook<FMA, FMB>>(get_market_addr<FMA, FMB>());
            let order = get_order<FMA, FMB>(book, sellID);
            assert!(order.metadata.status == STATUS_FILLED, 0);
            assert!(coin::value(&order.buyCollateral) == 0, 0);
            assert!(coin::value(&order.sellCollateral) == 0, 0);
            assert!(coin::balance<FMB>(address_of(user1)) == 50000000002, 0);
            assert!(coin::balance<FMA>(address_of(user1)) == 49999990000, 0);
        };
    }

    #[test(owner = @ferum, aptos = @0x1)]
    fun test_quote(owner: &signer, aptos: &signer) acquires OrderBook, UserMarketInfo {
        // Tests quote is set correctly given an orderbook state.

        account::create_account_for_test(address_of(owner));
        create_fake_coins(owner, 8);
        register_fma(owner, owner, 50000000000);
        register_fmb(owner, owner, 50000000000);
        setup_market_for_test<FMA, FMB>(owner, aptos);

        // Book setup.
        add_limit_order_entry<FMA, FMB>(owner, SIDE_BUY, 1, 2, empty_client_order_id()); // BUY 0.0002 FMA @ 0.0001 FMB
        add_limit_order_entry<FMA, FMB>(owner, SIDE_BUY, 2, 2, empty_client_order_id()); // BUY 0.0002 FMA @ 0.0002 FMB
        add_limit_order_entry<FMA, FMB>(owner, SIDE_BUY, 2, 2, empty_client_order_id()); // BUY 0.0002 FMA @ 0.0002 FMB
        add_limit_order_entry<FMA, FMB>(owner, SIDE_SELL, 3, 1, empty_client_order_id()); // SELL 0.0001 FMA @ 0.0003 FMB
        add_limit_order_entry<FMA, FMB>(owner, SIDE_SELL, 3, 1, empty_client_order_id()); // SELL 0.0001 FMA @ 0.0003 FMB
        add_limit_order_entry<FMA, FMB>(owner, SIDE_SELL, 4, 1, empty_client_order_id()); // SELL 0.0001 FMA @ 0.0004 FMB

        // Validate quote.
        let book = borrow_global<OrderBook<FMA, FMB>>(get_market_addr<FMA, FMB>());
        let price = get_quote(book);
        let expectedPrice = Quote {
            instrumentType: type_info::type_of<Quote>(),
            quoteType: type_info::type_of<Quote>(),

            maxBid: fixed_point_64::from_u128(2, 4),
            bidSize: fixed_point_64::from_u128(4, 4),
            minAsk: fixed_point_64::from_u128(3, 4),
            askSize: fixed_point_64::from_u128(2, 4),
            timestampMicroSeconds: 10,
        };
        assert_quote_eq(price, expectedPrice);
    }

    #[test(owner = @ferum, aptos = @0x1)]
    fun test_quote_empty_book(owner: &signer, aptos: &signer) acquires OrderBook {
        // Tests quote is set correctly given an empty orderbook.

        account::create_account_for_test(address_of(owner));
        create_fake_coins(owner, 8);
        register_fma(owner, owner, 50000000000);
        register_fmb(owner, owner, 50000000000);
        setup_market_for_test<FMA, FMB>(owner, aptos);

        // Validate quote.
        let book = borrow_global<OrderBook<FMA, FMB>>(get_market_addr<FMA, FMB>());
        let price = get_quote(book);
        let expectedPrice = Quote {
            instrumentType: type_info::type_of<Quote>(),
            quoteType: type_info::type_of<Quote>(),
            maxBid: fixed_point_64::zero(),
            bidSize: fixed_point_64::zero(),
            minAsk: fixed_point_64::zero(),
            askSize: fixed_point_64::zero(),
            timestampMicroSeconds: 10,
        };
        assert_quote_eq(price, expectedPrice);
    }

    #[test(owner = @ferum, aptos = @0x1)]
    fun test_quote_empty_sell_book(owner: &signer, aptos: &signer) acquires OrderBook, UserMarketInfo {
        // Tests quote is set correctly given an empty sell orderbook.

        account::create_account_for_test(address_of(owner));
        create_fake_coins(owner, 8);
        register_fma(owner, owner, 50000000000);
        register_fmb(owner, owner, 50000000000);
        setup_market_for_test<FMA, FMB>(owner, aptos);

        // Book setup.
        add_limit_order_entry<FMA, FMB>(owner, SIDE_BUY, 1, 2, empty_client_order_id()); // BUY 0.0002 FMA @ 0.0001 FMB
        add_limit_order_entry<FMA, FMB>(owner, SIDE_BUY, 2, 2, empty_client_order_id()); // BUY 0.0002 FMA @ 0.0002 FMB
        add_limit_order_entry<FMA, FMB>(owner, SIDE_BUY, 2, 2, empty_client_order_id()); // BUY 0.0002 FMA @ 0.0002 FMB

        // Validate quote.
        let book = borrow_global<OrderBook<FMA, FMB>>(get_market_addr<FMA, FMB>());
        let price = get_quote(book);
        let expectedPrice = Quote {
            instrumentType: type_info::type_of<Quote>(),
            quoteType: type_info::type_of<Quote>(),
            maxBid: fixed_point_64::from_u128(2, 4),
            bidSize: fixed_point_64::from_u128(4, 4),
            minAsk: fixed_point_64::zero(),
            askSize: fixed_point_64::zero(),
            timestampMicroSeconds: 10,
        };
        assert_quote_eq(price, expectedPrice);
    }

    #[test(owner = @ferum, aptos = @0x1)]
    fun test_quote_empty_buy_book(owner: &signer, aptos: &signer) acquires OrderBook, UserMarketInfo {
        // Tests quote is set correctly given an empty sell orderbook.

        account::create_account_for_test(address_of(owner));
        create_fake_coins(owner, 8);
        register_fma(owner, owner, 50000000000);
        register_fmb(owner, owner, 50000000000);
        setup_market_for_test<FMA, FMB>(owner, aptos);

        // Book setup.
        add_limit_order_entry<FMA, FMB>(owner, SIDE_SELL, 3, 1, empty_client_order_id()); // SELL 0.0001 FMA @ 0.0003 FMB
        add_limit_order_entry<FMA, FMB>(owner, SIDE_SELL, 3, 1, empty_client_order_id()); // SELL 0.0001 FMA @ 0.0003 FMB
        add_limit_order_entry<FMA, FMB>(owner, SIDE_SELL, 4, 1, empty_client_order_id()); // SELL 0.0001 FMA @ 0.0004 FMB

        // Validate quote.
        let book = borrow_global<OrderBook<FMA, FMB>>(get_market_addr<FMA, FMB>());
        let price = get_quote(book);
        let expectedPrice = Quote {
            instrumentType: type_info::type_of<Quote>(),
            quoteType: type_info::type_of<Quote>(),

            maxBid: fixed_point_64::zero(),
            bidSize: fixed_point_64::zero(),
            minAsk: fixed_point_64::from_u128(3, 4),
            askSize: fixed_point_64::from_u128(2, 4),
            timestampMicroSeconds: 10,
        };
        assert_quote_eq(price, expectedPrice);
    }

    #[test_only]
    // Ignores timestamp.
    fun assert_quote_eq(quote: Quote, expected: Quote) {
        assert!(fixed_point_64::eq(quote.maxBid, expected.maxBid), 0);
        assert!(fixed_point_64::eq(quote.bidSize, expected.bidSize), 0);
        assert!(fixed_point_64::eq(quote.minAsk, expected.minAsk), 0);
        assert!(fixed_point_64::eq(quote.askSize, expected.askSize), 0);
    }

    #[test_only]
    fun setup_market_for_test<I, Q>(owner: &signer, aptos: &signer) {
        timestamp::set_time_has_started_for_testing(aptos);
        init_ferum(owner);
        init_market_entry<I, Q>(owner, 4, 4);
    }

    #[test_only]
    fun get_order<I, Q>(book: &OrderBook<I, Q>, orderID: OrderID): &Order<I, Q> {
        let orderMap = &book.orderMap;
        let contains = table::contains(orderMap, orderID);
        if (contains) {
            table::borrow(orderMap, orderID)
        } else {
            table::borrow(&book.finalizedOrderMap, orderID)
        }
    }

    #[test_only]
    fun empty_client_order_id(): String {
        string::utf8(b"")
    }
}