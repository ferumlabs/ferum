
```javascript
module ferum::market {
    use aptos_framework::coin;
    use aptos_std::event::{EventHandle, emit_event};
    use aptos_framework::account::{new_event_handle};
    use aptos_std::table;
    use std::vector;
    use std::signer::address_of;

    use ferum::ferum::{init_ferum, register_market, get_market_addr};
    use ferum::fixedpoint::{Self, FixedPoint};
    use ferum::list;

    #[test_only]
    use ferum::coin_test_helpers::{FMA, FMB, setup_fake_coins, register_fmb, register_fma};
    use ferum::utils::min_u8;
    #[test_only]
    use aptos_framework::account;
```

Errors


```javascript
    const ERR_NOT_ALLOWED: u64 = 0;
    const ERR_NOT_ADMIN: u64 = 1;
    const ERR_BOOK_EXISTS: u64 = 2;
    const ERR_BOOK_NOT_EXISTS: u64 = 3;
    const ERR_COIN_UNINITIALIZED: u64 = 4;
    const ERR_UNKNOWN_ORDER: u64 = 5;
    const ERR_INVALID_PRICE: u64 = 6;
    const ERR_NOT_OWNER: u64 = 7;
    const ERR_COIN_EXCEEDS_MAX_SUPPORTED_DECIMALS: u64 = 8;
    const ERR_INVALID_TYPE: u64 = 9;
    const ERR_NO_PROGRESS: u64 = 10;
    const ERR_MARKET_ORDER_NOT_PENDING: u64 = 11;
    const ERR_INVALID_DECIMAL_CONFIG: u64 = 12;
```

Enums.


```javascript
    const SIDE_SELL: u8 = 0;
    const SIDE_BUY: u8 = 1;

    const TYPE_MARKET: u8 = 0;
    const TYPE_LIMIT: u8 = 1;

    const STATUS_PENDING: u8 = 0; // Order not finalized.
    const STATUS_CANCELLED: u8 = 1; // Order finalized.
    const STATUS_PARTIALLY_FILLED: u8 = 2; // Order finalized.
    const STATUS_FILLED: u8 = 3; // Order finalized.

    const CANCEL_AGENT_NONE: u8 = 0;
    const CANCEL_AGENT_IOC: u8 = 1;
    const CANCEL_AGENT_USER: u8 = 2;
```

Constants.


```javascript
    const MAX_DECIMALS: u8 = 10;
```

Structs.


```javascript
    struct OrderMetadata has drop, store, copy {
        side: u8,
        remainingQty: FixedPoint,
        originalQty: FixedPoint,
        price: FixedPoint,
        type: u8,
        status: u8,
    }

    struct Order<phantom I, phantom Q> has store {
        id: u128,
        owner: address,
        metadata: OrderMetadata,
        buyCollateral: coin::Coin<Q>,
        sellCollateral: coin::Coin<I>,
    }

    struct OrderBook<phantom I, phantom Q> has key, store {
```
Order IDs of marker orders.

```javascript
        marketOrders: vector<u128>,
```
Order IDs stored in order of decreasing price.

```javascript
        sells: vector<u128>,
```
Order IDs stored in order of increasing price.

```javascript
        buys: vector<u128>,
```
Map of all non finalized orders.

```javascript
        orderMap: table::Table<u128, Order<I, Q>>,
```
Map of all finalized orders.

```javascript
        finalizedOrderMap: table::Table<u128, Order<I, Q>>,
```
Counter to generate order ID.

```javascript
        idCounter: u128,
```
Number of decimals for the instrument coin.

```javascript
        iDecimals: u8,
```
Number of decimals for the quote coin.

```javascript
        qDecimals: u8,
```
Finalize order events for this market.

```javascript
        finalizeEvents: EventHandle<FinalizeEvent>,
```
Execution events for this market.

```javascript
        executionEvents: EventHandle<ExecutionEvent>,
```
Create order events for this market.

```javascript
        createOrderEvents: EventHandle<CreateEvent>,
    }
```

Events.


```javascript
    struct ExecutionEvent has drop, store {
        orderID: u128,
        owner: address,
        orderMetadata: OrderMetadata,

        oppositeOrderID: u128,
        oppositeOwner: address,
        oppositeOrderMetadata: OrderMetadata,

        price: FixedPoint,
        qty: FixedPoint,
    }

    struct FinalizeEvent has drop, store {
        orderID: u128,
        owner: address,
        orderMetadata: OrderMetadata,
        cancelAgent: u8,
    }

    struct CreateEvent has drop, store {
        orderID: u128,
        owner: address,
        orderMetadata: OrderMetadata,
    }

    public entry fun init_market<I, Q>(owner: &signer, instrumentDecimals: u8, quoteDecimals: u8) {
        let ownerAddr = address_of(owner);
        assert!(!exists<OrderBook<I, Q>>(ownerAddr), ERR_BOOK_EXISTS);
        let (iCoinDecimals, qCoinDecimals) = validate_coins<I, Q>();
        assert!(iCoinDecimals >= instrumentDecimals && qCoinDecimals >= quoteDecimals, ERR_INVALID_DECIMAL_CONFIG);
        assert!(instrumentDecimals + quoteDecimals <= min_u8(iCoinDecimals, qCoinDecimals), ERR_INVALID_DECIMAL_CONFIG);

        let finalizeEvents = new_event_handle<FinalizeEvent>(owner);
        let createOrderEvents = new_event_handle<CreateEvent>(owner);
        let executionEvents = new_event_handle<ExecutionEvent>(owner);

        let book = OrderBook<I, Q>{
            marketOrders: vector::empty(),
            sells: vector::empty(),
            buys: vector::empty(),
            orderMap: table::new<u128, Order<I, Q>>(),
            finalizedOrderMap: table::new<u128, Order<I, Q>>(),
            idCounter: 0,
            iDecimals: instrumentDecimals,
            qDecimals: quoteDecimals,
            finalizeEvents,
            createOrderEvents,
            executionEvents,

        };
        move_to(owner, book);

        register_market<I, Q>(ownerAddr);
    }

    public entry fun add_limit_order<I, Q>(
        owner: &signer,
        side: u8,
        price: u64,
        qty: u64,
    ) acquires OrderBook {
        add_limit_order_internal<I, Q>(owner, side, price, qty);
    }

    public entry fun add_market_order<I, Q>(
        owner: &signer,
        side: u8,
        qty: u64,
        maxCollateralAmt: u64,
    ) acquires OrderBook {
        add_market_order_internal<I, Q>(owner, side, qty, maxCollateralAmt);
    }

    public entry fun cancel_order<I, Q>(owner: &signer, orderID: u128) acquires OrderBook {
        let bookAddr = get_market_addr<I, Q>();
        assert!(exists<OrderBook<I, Q>>(bookAddr), ERR_BOOK_NOT_EXISTS);

        let book = borrow_global_mut<OrderBook<I, Q>>(bookAddr);
        assert!(table::contains(&book.orderMap, orderID), ERR_UNKNOWN_ORDER);
        let order = table::borrow_mut(&mut book.orderMap, orderID);
        let ownerAddr = address_of(owner);
        assert!(ownerAddr == order.owner, ERR_NOT_OWNER);
        mark_cancelled_order(
            &mut book.finalizeEvents,
            ownerAddr,
            order,
            CANCEL_AGENT_USER,
        );
        clean_orders(book);
    }
```

Private functions.



```javascript
    fun add_limit_order_internal<I, Q>(
        owner: &signer,
        side: u8,
        price: u64,
        qty: u64,
    ): u128 acquires OrderBook {
        let bookAddr = get_market_addr<I, Q>();
        assert!(exists<OrderBook<I, Q>>(bookAddr), ERR_BOOK_NOT_EXISTS);
        validate_coins<I, Q>();

        let ownerAddr = address_of(owner);
        let book = borrow_global_mut<OrderBook<I, Q>>(bookAddr);

        let priceFixedPoint = fixedpoint::from_u64(price, book.qDecimals);
        let qtyFixedPoint = fixedpoint::from_u64(qty, book.iDecimals);

        let (buyCollateral, sellCollateral) = obtain_limit_order_collateral<I, Q>(
            owner,
            side,
            priceFixedPoint,
            qtyFixedPoint,
        );
        let orderID = gen_order_id(book);
        let order = Order<I, Q>{
            id: orderID,
            owner: ownerAddr,
            buyCollateral,
            sellCollateral,
            metadata: OrderMetadata{
                side,
                price: priceFixedPoint,
                remainingQty: qtyFixedPoint,
                type: TYPE_LIMIT,
                originalQty: qtyFixedPoint,
                status: STATUS_PENDING,
            },
        };

        add_order<I, Q>(owner, book, order);

        orderID
    }

    fun add_market_order_internal<I, Q>(
        owner: &signer,
        side: u8,
        qty: u64,
        maxCollateralAmt: u64,
    ): u128 acquires OrderBook {
        let bookAddr = get_market_addr<I, Q>();
        assert!(exists<OrderBook<I, Q>>(bookAddr), ERR_BOOK_NOT_EXISTS);
        validate_coins<I, Q>();

        let book = borrow_global_mut<OrderBook<I, Q>>(bookAddr);

        let qtyFixedPoint = fixedpoint::from_u64(qty, book.iDecimals);

        let ownerAddr = address_of(owner);

        let (buyCollateral, sellCollateral) = obtain_market_order_collateral<I, Q>(
            owner,
            side,
            qtyFixedPoint,
            maxCollateralAmt,
        );

        let orderID = gen_order_id(book);
        let order = Order<I, Q>{
            id: orderID,
            owner: ownerAddr,
            buyCollateral,
            sellCollateral,
            metadata: OrderMetadata{
                side,
                price: fixedpoint::zero(),
                remainingQty: qtyFixedPoint,
                type: TYPE_MARKET,
                originalQty: qtyFixedPoint,
                status: STATUS_PENDING,
            },
        };

        add_order(owner, book, order);

        orderID
    }

    fun gen_order_id<I, Q>(book: &mut OrderBook<I, Q>): u128 {
        let id = book.idCounter;
        book.idCounter = book.idCounter + 1;
        id
    }

    fun add_order<I, Q>(owner: &signer, book: &mut OrderBook<I, Q>, order: Order<I, Q>) {
        validate_order(&order);

        emit_order_created_event(owner, book, &order);

        if (order.metadata.type == TYPE_MARKET) {
            vector::push_back(&mut book.marketOrders, order.id);
        } else if (order.metadata.side == SIDE_BUY) {
            add_order_to_list(&order, &mut book.buys, &book.orderMap);
        } else {
            add_order_to_list(&order, &mut book.sells, &book.orderMap);
        };

        let orderMap = &mut book.orderMap;
        table::add(orderMap, order.id, order);

        process_orders(book)
    }

    fun process_orders<I, Q>(book: &mut OrderBook<I, Q>) {
        let buys = &book.buys;
        let sells = &book.sells;
        let marketOrders = &book.marketOrders;
        let orderMap = &mut book.orderMap;

        // Process market orders first.
        if (vector::length(marketOrders) > 0) {
            let i = vector::length(marketOrders) - 1;
            loop {
                let order = get_order_from_list_mut(orderMap, marketOrders, i);
                if (order.metadata.side == SIDE_BUY) {
                    execute_market_order(
                        &mut book.executionEvents,
                        &mut book.finalizeEvents,
                        orderMap,
                        sells,
                        order.id,
                    )
                }
                else {
                    execute_market_order(
                        &mut book.executionEvents,
                        &mut book.finalizeEvents,
                        orderMap,
                        buys,
                        order.id,
                    )
                };

                if (i == 0) {
                    break
                };
                i = i - 1;
            }
        };

        clean_orders(book);

        // Re borrow.
        buys = &book.buys;
        sells = &book.sells;

        // If there are no orders in any one sides, we can return.
        if (vector::length(buys) == 0 || vector::length(sells) == 0) {
            return
        };

        execute_limit_orders(book);

        clean_orders(book);
    }

    fun get_order_from_list_mut<I, Q>(
        orderMap: &mut table::Table<u128, Order<I, Q>>,
        list: &vector<u128>,
        i: u64,
    ): &mut Order<I, Q> {
        let orderID = *vector::borrow(list, i);
        table::borrow_mut(orderMap, orderID)
    }

    fun get_order_from_list<I, Q>(
        orderMap: &table::Table<u128, Order<I, Q>>,
        list: &vector<u128>,
        i: u64,
    ): &Order<I, Q> {
        let orderID = *vector::borrow(list, i);
        table::borrow(orderMap, orderID)
    }

    fun add_order_to_list<I, Q>(
        order: &Order<I, Q>,
        orderList: &mut vector<u128>,
        orderMap: &table::Table<u128, Order<I, Q>>,
    ) {
        if (vector::length(orderList) == 0) {
            vector::push_back(orderList, order.id);
            return
        };

        let orderMetadata = &order.metadata;

        let i = vector::length(orderList) - 1;
        loop {
            let orderID = *vector::borrow(orderList, i);
            let listOrder = table::borrow(orderMap, orderID);
            let listOrderMetadata = &listOrder.metadata;

            if (orderMetadata.side == SIDE_BUY) {
                if (fixedpoint::lte(listOrderMetadata.price, orderMetadata.price)) {
                    list::insert(orderList, i+1, order.id);
                    break
                }
            }
            else {
                if (fixedpoint::gte(listOrderMetadata.price, orderMetadata.price)) {
                    list::insert(orderList, i+1, order.id);
                    break
                }
            };

            if (i == 0) {
                list::insert(orderList, 0, order.id);
                break
            };
            i = i - 1;
        };
    }

    fun execute_limit_orders<I, Q>(book: &mut OrderBook<I, Q>) {
        let orderMap = &mut book.orderMap;
        let buys = &book.buys;
        let sells = &book.sells;

        let maxBidIdx = vector::length(buys) - 1;
        let maxBidPrice = get_order_from_list(orderMap, buys, maxBidIdx).metadata.price;
        let minAskIdx = vector::length(sells) - 1;
        let minAskPrice = get_order_from_list(orderMap, sells, minAskIdx).metadata.price;

        while (fixedpoint::gte(maxBidPrice, minAskPrice) && maxBidIdx >= 0 && minAskIdx >= 0) {
            let maxBid = get_order_from_list(orderMap, buys, maxBidIdx);
            let maxBidID = maxBid.id;
            let minAsk = get_order_from_list(orderMap, sells, minAskIdx);
            let minAskID = minAsk.id;
            // Shouldn't need to worry about over executing collateral because the minAsk price is less than the
            // maxBid price.
            let executedQty = fixedpoint::min(maxBid.metadata.remainingQty, minAsk.metadata.remainingQty);

            let maxBidMut = get_order_from_list_mut(orderMap, buys, maxBidIdx);
            maxBidMut.metadata.remainingQty = fixedpoint::sub(maxBidMut.metadata.remainingQty, executedQty);
            let maxBidMetadata = maxBidMut.metadata;
            let maxBidOwner = maxBidMut.owner;

            let minAskMut = get_order_from_list_mut(orderMap, sells, minAskIdx);
            minAskMut.metadata.remainingQty = fixedpoint::sub(minAskMut.metadata.remainingQty, executedQty);
            let minAskMetadata = minAskMut.metadata;
            let minAskOwner = minAskMut.owner;

            // Give the midpoint for the price.
            // TODO: add fee.
            let price = fixedpoint::divide_round_up(
                fixedpoint::add(minAskPrice, maxBidPrice),
                fixedpoint::from_u64(2, 0),
            );
            // Its possible for the midpoint to have more decimal places than the market allows for quotes.
            // In this case, round up.
            price = fixedpoint::round_up_to_decimals(price, book.qDecimals);

            swap_collateral(orderMap, maxBidID, minAskID, price, executedQty);

            emit_event(&mut book.executionEvents, ExecutionEvent {
                orderID: maxBidID,
                owner: maxBidOwner,
                orderMetadata: maxBidMetadata,

                oppositeOrderID: minAskID,
                oppositeOwner: minAskOwner,
                oppositeOrderMetadata: minAskMetadata,

                price,
                qty: executedQty,
            });

            emit_event(&mut book.executionEvents, ExecutionEvent {
                orderID: minAskID,
                owner: minAskOwner,
                orderMetadata: minAskMetadata,

                oppositeOrderID: maxBidID,
                oppositeOwner: maxBidOwner,
                oppositeOrderMetadata: maxBidMetadata,

                price,
                qty: executedQty,
            });

            // Update order status.
            {
                let maxBidMut = get_order_from_list_mut(orderMap, buys, maxBidIdx);
                if (finalize_order_if_needed(&mut book.finalizeEvents, maxBidMut)) {
                    if (maxBidIdx == 0) {
                        break
                    };
                    maxBidIdx = maxBidIdx - 1;
                    maxBidPrice = get_order_from_list(orderMap, buys, maxBidIdx).metadata.price;
                }
            };
            {
                let minAskMut = get_order_from_list_mut(orderMap, sells, minAskIdx);
                if (finalize_order_if_needed(&mut book.finalizeEvents, minAskMut)) {
                    if (minAskIdx == 0) {
                        break
                    };
                    minAskIdx = minAskIdx - 1;
                    minAskPrice = get_order_from_list(orderMap, sells, minAskIdx).metadata.price;
                };
            };
        };
    }

    fun execute_market_order<I, Q>(
        execution_event_handle: &mut EventHandle<ExecutionEvent >,
        finalize_event_handle: &mut EventHandle<FinalizeEvent>,
        orderMap: &mut table::Table<u128, Order<I, Q>>,
        orderList: &vector<u128>,
        orderID: u128,
    ) {
        if (vector::length(orderList) == 0) {
            let order =  table::borrow_mut(orderMap, orderID);
            assert!(order.metadata.status == STATUS_PENDING, ERR_MARKET_ORDER_NOT_PENDING);
            mark_cancelled_order(finalize_event_handle, order.owner, order, CANCEL_AGENT_IOC);
            return
        };

        let i = vector::length(orderList) - 1;
        loop {
            let bookOrderID = *vector::borrow(orderList, i);
            let executedQty = {
                let order =  table::borrow(orderMap, orderID);
                let bookOrder = table::borrow(orderMap, bookOrderID);

                if (order.metadata.side == SIDE_BUY) {
                    // For a buy order, we need to factor in the remaining collateral for the order when deciding what
                    // the execution qty should be.
                    let remainingCollateral = get_remaining_collateral(order);
                    let maxQtyAtPrice = fixedpoint::divide_trunc(remainingCollateral, bookOrder.metadata.price);
                    let remainingQty =  fixedpoint::min(
                        bookOrder.metadata.remainingQty,
                        order.metadata.remainingQty,
                    );
                    fixedpoint::min(maxQtyAtPrice, remainingQty)
                } else {
                    // For a sell order, we can just use the remaining quantity.
                    fixedpoint::min(
                        bookOrder.metadata.remainingQty,
                        order.metadata.remainingQty,
                    )
                }
            };

            let bookOrder = table::borrow_mut(orderMap, bookOrderID);
            bookOrder.metadata.remainingQty = fixedpoint::sub(bookOrder.metadata.remainingQty, executedQty);
            let bookOrderID = bookOrder.id;
            let bookOrderOwner = bookOrder.owner;
            let bookOrderMetadata = bookOrder.metadata;

            let order = table::borrow_mut(orderMap, orderID);
            order.metadata.remainingQty = fixedpoint::sub(order.metadata.remainingQty, executedQty);
            let orderID = order.id;
            let orderOwner = order.owner;
            let orderMetadata = order.metadata;

            if (order.metadata.side == SIDE_BUY) {
                swap_collateral(
                    orderMap,
                    orderID,
                    bookOrderID,
                    bookOrderMetadata.price,
                    executedQty,
                );
            } else {
                swap_collateral(
                    orderMap,
                    bookOrderID,
                    orderID,
                    bookOrderMetadata.price,
                    executedQty,
                );
            };

            emit_event(execution_event_handle, ExecutionEvent {
                orderID,
                owner: orderOwner,
                orderMetadata,

                oppositeOrderID: bookOrderID,
                oppositeOwner: bookOrderOwner,
                oppositeOrderMetadata: bookOrderMetadata,

                price: bookOrderMetadata.price,
                qty: executedQty,
            });

            emit_event(execution_event_handle, ExecutionEvent {
                orderID: bookOrderID,
                owner: bookOrderOwner,
                orderMetadata: bookOrderMetadata,

                oppositeOrderID: orderID,
                oppositeOwner: orderOwner,
                oppositeOrderMetadata: orderMetadata,

                price: bookOrderMetadata.price,
                qty: executedQty,
            });

            bookOrder = table::borrow_mut(orderMap, bookOrderID); // Re-borrow.
            finalize_order_if_needed(finalize_event_handle, bookOrder);

            order = table::borrow_mut(orderMap, orderID); // Re-borrow.
            if (finalize_order_if_needed(finalize_event_handle, order)) {
                break
            };
            if (i == 0) {
                break
            };
            i = i - 1;
        };

        let order =  table::borrow_mut(orderMap, orderID);
        if (order.metadata.status != STATUS_FILLED && order.metadata.status != STATUS_PARTIALLY_FILLED) {
            mark_cancelled_order(finalize_event_handle, order.owner, order, CANCEL_AGENT_IOC);
        }
    }

    fun clean_orders<I, Q>(book: &mut OrderBook<I, Q>) {
        clean_orders_internal(&mut book.orderMap, &mut book.finalizedOrderMap, &mut book.sells);
        clean_orders_internal(&mut book.orderMap, &mut book.finalizedOrderMap, &mut book.buys);
        clean_orders_internal(&mut book.orderMap, &mut book.finalizedOrderMap, &mut book.marketOrders);
    }

    fun clean_orders_internal<I, Q>(
        orderMap: &mut table::Table<u128, Order<I, Q>>,
        finalizedOrderMap: &mut table::Table<u128, Order<I, Q>>,
        orderList: &mut vector<u128>,
    ) {
        let count = vector::length(orderList);
        if (count == 0) {
            return
        };
        let i = count - 1;
        loop {
            let orderID = *vector::borrow(orderList, i);
            let order = table::borrow(orderMap, orderID);
            let isFinalized = (
                order.metadata.status == STATUS_FILLED ||
                order.metadata.status == STATUS_PARTIALLY_FILLED ||
                order.metadata.status == STATUS_CANCELLED
            );
            if (isFinalized) {
                vector::remove(orderList, i);
                let orderOwner = order.owner;
                let order = table::remove(orderMap, orderID);

                let buyCollateral = coin::extract_all(&mut order.buyCollateral);
                let sellCollateral = coin::extract_all(&mut order.sellCollateral);

                table::add(finalizedOrderMap, order.id, order);

                // Return any remaining collateral to user.
                coin::deposit(orderOwner, buyCollateral);
                coin::deposit(orderOwner, sellCollateral);
            };
            if (i == 0) {
                break
            };
            i = i - 1;
        };
    }
```

Validation functions.


```javascript
    fun validate_order<I, Q>(order: &Order<I, Q>) {
        let metadata = &order.metadata;
        if (metadata.type == TYPE_MARKET) {
            assert!(fixedpoint::eq(metadata.price, fixedpoint::zero()), ERR_INVALID_PRICE);
        }
        else if (metadata.type == TYPE_LIMIT) {
            assert!(fixedpoint::gt(metadata.price, fixedpoint::zero()), ERR_INVALID_PRICE);
        };
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
```

Order specific helpers.


```javascript
    fun finalize_order_if_needed<I, Q>(
        finalize_event_handle: &mut EventHandle<FinalizeEvent>,
        order: &mut Order<I, Q>,
    ): bool {
        let hasCollateral = has_remaining_collateral(order);
        let hasQty = has_remaining_qty(order);
        if (!hasCollateral || !hasQty) {
            order.metadata.status = if (hasQty) STATUS_PARTIALLY_FILLED else STATUS_FILLED;
            emit_event(finalize_event_handle, FinalizeEvent{
                owner: order.owner,
                orderID: order.id,
                orderMetadata: order.metadata,
                cancelAgent: CANCEL_AGENT_NONE,
            });
            return true
        };
        false
    }

    fun emit_order_created_event<I, Q>(owner: &signer, book: &mut OrderBook<I, Q>, order: &Order<I, Q>) {
        emit_event(&mut book.createOrderEvents, CreateEvent{
            orderID: order.id,
            owner: address_of(owner),
            orderMetadata: order.metadata,
        });
    }

    fun has_remaining_qty<I, Q>(order: &Order<I, Q>): bool {
        !fixedpoint::eq(order.metadata.remainingQty, fixedpoint::zero())
    }

    fun has_remaining_collateral<I, Q>(order: &Order<I, Q>): bool {
        coin::value(&order.buyCollateral) > 0 || coin::value(&order.sellCollateral) > 0
    }

    fun get_remaining_collateral<I, Q>(order: &Order<I, Q>): FixedPoint {
        if (order.metadata.side == SIDE_BUY) {
            let coinDecimals = coin::decimals<Q>();
            fixedpoint::from_u64(coin::value(&order.buyCollateral), coinDecimals)
        } else {
            let coinDecimals = coin::decimals<I>();
            fixedpoint::from_u64(coin::value(&order.sellCollateral), coinDecimals)
        }
    }

    public fun mark_cancelled_order<I, Q>(
        finalize_event_handle: &mut EventHandle<FinalizeEvent>,
        owner: address,
        order: &mut Order<I, Q>,
        cancelAgent: u8,
    ) {
        order.metadata.status = STATUS_CANCELLED;
        emit_event(finalize_event_handle, FinalizeEvent{
            orderID: order.id,
            owner,
            orderMetadata: order.metadata,
            cancelAgent,
        })
    }

    fun drop_order<I, Q>(order: Order<I, Q>): (coin::Coin<Q>, coin::Coin<I>) {
        let Order<I, Q>{id: _, metadata: _, owner: _, buyCollateral, sellCollateral} = order;
        (buyCollateral, sellCollateral)
    }
```

Collateral functions.


```javascript
    fun obtain_limit_order_collateral<I, Q>(
        owner: &signer,
        side: u8,
        price: FixedPoint,
        qty: FixedPoint,
    ): (coin::Coin<Q>, coin::Coin<I>) {
        if (side == SIDE_BUY) {
            (
                coin::withdraw<Q>(
                    owner,
                    fixedpoint::to_u64(fixedpoint::multiply_round_up(price, qty), coin::decimals<Q>()),
                ),
                coin::zero<I>(),
            )
        } else {
            (
                coin::zero<Q>(),
                coin::withdraw<I>(
                    owner,
                    fixedpoint::to_u64(qty, coin::decimals<I>()),
                ),
            )
        }
    }

    fun obtain_market_order_collateral<I, Q>(
        owner: &signer,
        side: u8,
        qty: FixedPoint,
        maxCollateralAmt: u64,
    ): (coin::Coin<Q>, coin::Coin<I>) {
        if (side == SIDE_BUY) {
            (coin::withdraw<Q>(owner, maxCollateralAmt), coin::zero<I>())
        } else {
            let coinDecimals = coin::decimals<I>();
            (coin::zero<Q>(), coin::withdraw<I>(owner,  fixedpoint::to_u64(qty, coinDecimals)))
        }
    }
```
Moves collateral from orders to owners based on the execution details.

```javascript
    fun swap_collateral<I, Q>(
        orderMap: &mut table::Table<u128, Order<I, Q>>,
        buyID: u128,
        sellID: u128,
        price: FixedPoint,
        qty: FixedPoint,
    ) {
        {
            let sell = table::borrow(orderMap, sellID);
            let sellOwner = sell.owner;
            let buy = table::borrow_mut(orderMap, buyID);
            let buyCollateral = extract_buy_collateral(buy, price, qty);
            coin::deposit(sellOwner, buyCollateral);
        };

        {
            let buy = table::borrow(orderMap, buyID);
            let buyOwner = buy.owner;
            let sell = table::borrow_mut(orderMap, sellID);
            let sellCollateral = extract_sell_collateral(sell, qty);
            coin::deposit(buyOwner, sellCollateral);
        };
    }

    fun extract_buy_collateral<I, Q>(order: &mut Order<I, Q>, price: FixedPoint, qty: FixedPoint): coin::Coin<Q> {
        let collateralUsed = fixedpoint::multiply_trunc(price, qty);
        let coinDecimals = coin::decimals<Q>();
        let amt = fixedpoint::to_u64(collateralUsed, coinDecimals);
        coin::extract(&mut order.buyCollateral, amt)
    }

    fun extract_sell_collateral<I, Q>(order: &mut Order<I, Q>, qty: FixedPoint): coin::Coin<I> {
        let coinDecimals = coin::decimals<I>();
        let amt = fixedpoint::to_u64(qty, coinDecimals);
        coin::extract(&mut order.sellCollateral, amt)
    }

    #[test(owner = @ferum, user = @0x2)]
    #[expected_failure]
    fun test_init_market_with_duplicate_market<I, Q>(owner: &signer) {
        init_ferum(owner);
        init_market<I, Q>(owner, 4, 4);
        init_market<I, Q>(owner, 4, 4);
    }

    #[test(owner = @ferum, user = @0x2)]
    #[expected_failure]
    fun test_add_limit_order_to_uninited_book(owner: &signer, user: &signer) acquires OrderBook {
        // Tests that a limit order added for uninitialized book fails.

        account::create_account_for_test(address_of(owner));
        account::create_account_for_test(address_of(user));
        init_ferum(owner);
        setup_fake_coins(owner, user, 100, 18);
        add_limit_order<FMA, FMB>(owner, SIDE_SELL, 1, 1);
    }

    #[test(owner = @ferum, user = @0x2)]
    #[expected_failure]
    fun test_add_market_order_to_uninited_book(owner: &signer, user: &signer) acquires OrderBook {
        // Tests that a limit order added for uninitialized book fails.

        account::create_account_for_test(address_of(owner));
        account::create_account_for_test(address_of(user));
        init_ferum(owner);
        setup_fake_coins(owner, user, 100, 18);
        add_market_order<FMA, FMB>(owner, SIDE_SELL, 1, 1);
    }

    #[test(owner = @ferum, user = @0x2)]
    #[expected_failure]
    fun test_add_buy_order_exceed_balance(owner: &signer, user: &signer) acquires OrderBook {
        // Tests that a buy order that requires more collateral than the user has fails.

        account::create_account_for_test(address_of(owner));
        account::create_account_for_test(address_of(user));
        setup_fake_coins(owner, user, 10000000000, 8); // Users have 100 FMA and FMB.
        setup_market_for_test<FMA, FMB>(owner);

        add_limit_order<FMA, FMB>(owner, SIDE_BUY, 10000, 1200000); // BUY 120 FMA @ 1 FMB
    }

    #[test(owner = @ferum, user = @0x2)]
    #[expected_failure]
    fun test_add_buy_order_exceed_balance_price(owner: &signer, user: &signer) acquires OrderBook {
        // Tests that a buy order that requires more collateral than the user has fails.

        account::create_account_for_test(address_of(owner));
        account::create_account_for_test(address_of(user));
        setup_fake_coins(owner, user, 10000000000, 8); // Users have 100 FMA and FMB.
        setup_market_for_test<FMA, FMB>(owner);

        add_limit_order<FMA, FMB>(owner, SIDE_BUY, 1200000, 10000); // BUY 1 FMA @ 120 FMB
    }

    #[test(owner = @ferum, user = @0x2)]
    #[expected_failure]
    fun test_add_sell_order_exceed_balance(owner: &signer, user: &signer) acquires OrderBook {
        // Tests that a sell order that requires more collateral than the user has fails.

        account::create_account_for_test(address_of(owner));
        account::create_account_for_test(address_of(user));
        setup_fake_coins(owner, user, 10000000000, 8); // Users have 100 FMA and FMB.
        setup_market_for_test<FMA, FMB>(owner);

        add_limit_order<FMA, FMB>(owner, SIDE_SELL, 10000, 1200000); // SELL 120 FMA @ 1 FMB
    }

    #[test(owner = @ferum, user = @0x2)]
    fun test_add_sell_order_no_precision_loss(owner: &signer, user: &signer) acquires OrderBook {
        // Tests that a sell order placed with the minimum qty doesn't fail.

        account::create_account_for_test(address_of(owner));
        account::create_account_for_test(address_of(user));
        setup_fake_coins(owner, user, 10000000000, 8); // Users have 100 FMA and FMB.
        setup_market_for_test<FMA, FMB>(owner);

        // SELL 0.00000001 FMA @ 0.00000001 FMB
        // Requires obtaining 0.00000001 FMA of collateral, which is possible.
        add_limit_order<FMA, FMB>(owner, SIDE_SELL, 1, 1);
    }

    #[test(owner = @ferum, user = @0x2)]
    fun test_add_orders_to_empty_book(owner: &signer, user: &signer) acquires OrderBook {
        // Tests that orders can be added to empty book and none of them trigger.

        account::create_account_for_test(address_of(owner));
        account::create_account_for_test(address_of(user));
        setup_fake_coins(owner, user, 10000000000, 8);
        setup_market_for_test<FMA, FMB>(owner);

        add_limit_order<FMA, FMB>(owner, SIDE_BUY, 10000, 100000); // BUY 10 FMA @ 1 FMB
        add_limit_order<FMA, FMB>(owner, SIDE_BUY, 20000, 10000); // BUY 1 FMA @ 2 FMB
        add_limit_order<FMA, FMB>(owner, SIDE_BUY, 100000, 10000); // BUY 1 FMA @ 10 FMB

        add_limit_order<FMA, FMB>(owner, SIDE_SELL, 200000, 100000); // SELL 10 FMA @ 20 FMB
        add_limit_order<FMA, FMB>(owner, SIDE_SELL, 210000, 10000); // SELL 1 FMA @ 21 FMB
        add_limit_order<FMA, FMB>(owner, SIDE_SELL, 250000, 10000); // SELL 1 FMA @ 25 FMB

        assert!(coin::balance<FMA>(address_of(owner)) == 8800000000, 0);
        assert!(coin::balance<FMB>(address_of(owner)) == 7800000000, 0);
    }

    #[test(owner = @ferum, user = @0x2)]
    fun test_add_market_orders_cancelled(owner: &signer, user: &signer) acquires OrderBook {
        // Tests that market orders should get cancelled because there is nothing to execute them against.

        account::create_account_for_test(address_of(owner));
        account::create_account_for_test(address_of(user));
        setup_fake_coins(owner, user, 10000000000, 8);
        setup_market_for_test<FMA, FMB>(owner);

        // BUY 1 FMA spending at most 10 FMB
        {
            let orderID = add_market_order_internal<FMA, FMB>(
                owner,
                SIDE_BUY,
                10000,
                100000,
            );
            let book = borrow_global<OrderBook<FMA, FMB>>(get_market_addr<FMA, FMB>());
            let order = get_order<FMA, FMB>(book, orderID);
            assert!(order.metadata.status == STATUS_CANCELLED, 0);
            assert!(coin::value(&order.buyCollateral) == 0, 0);
            assert!(coin::value(&order.sellCollateral) == 0, 0);
        };

        // SELL 1 FMA.
        {
            let orderID = add_market_order_internal<FMA, FMB>(
                owner,
                SIDE_SELL,
                10000,
                0,
            );
            let book = borrow_global<OrderBook<FMA, FMB>>(get_market_addr<FMA, FMB>());
            let order = get_order<FMA, FMB>(book, orderID);
            assert!(order.metadata.status == STATUS_CANCELLED, 0);
            assert!(coin::value(&order.buyCollateral) == 0, 0);
            assert!(coin::value(&order.sellCollateral) == 0, 0);
        };
    }

    #[test(owner = @ferum, user1 = @0x2, user2 = @0x3)]
    fun test_market_buy_execute_against_limit(owner: &signer, user1: &signer, user2: &signer) acquires OrderBook {
        // Tests that market buy order execute against limit orders.

        account::create_account_for_test(address_of(owner));
        account::create_account_for_test(address_of(user1));
        account::create_account_for_test(address_of(user2));
        setup_fake_coins(owner, user1, 10000000000, 8);
        register_fma(owner, user2, 10000000000);
        register_fmb(owner, user2, 10000000000);
        setup_market_for_test<FMA, FMB>(owner);

        // Book setup.
        add_limit_order_internal<FMA, FMB>(owner, SIDE_BUY, 10000, 100000); // BUY 10 FMA @ 1 FMB
        add_limit_order_internal<FMA, FMB>(owner, SIDE_BUY, 20000, 10000); // BUY 1 FMA @ 2 FMB
        add_limit_order_internal<FMA, FMB>(owner, SIDE_BUY, 100000, 10000); // BUY 1 FMA @ 10 FMB

        let targetSellID = add_limit_order_internal<FMA, FMB>(user2, SIDE_SELL, 200000, 100000); // SELL 10 FMA @ 20 FMB
        add_limit_order_internal<FMA, FMB>(owner, SIDE_SELL, 210000, 10000); // SELL 1 FMA @ 21 FMB
        add_limit_order_internal<FMA, FMB>(owner, SIDE_SELL, 250000, 10000); // SELL 1 FMA @ 25 FMB

        // BUY 1 FMA spending at most 20 FMB.
        let orderID = add_market_order_internal<FMA, FMB>(
            user1,
            SIDE_BUY,
            10000,
            2000000000,
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

    #[test(owner = @ferum, user1 = @0x2, user2 = @0x3)]
    fun test_market_sell_execute_against_limit(owner: &signer, user1: &signer, user2: &signer) acquires OrderBook {
        // Tests that market sell order execute against limit orders.

        account::create_account_for_test(address_of(owner));
        account::create_account_for_test(address_of(user1));
        account::create_account_for_test(address_of(user2));
        setup_fake_coins(owner, user1, 10000000000, 8);
        register_fma(owner, user2, 10000000000);
        register_fmb(owner, user2, 10000000000);
        setup_market_for_test<FMA, FMB>(owner);

        // Book setup.
        add_limit_order_internal<FMA, FMB>(owner, SIDE_BUY, 10000, 100000); // BUY 10 FMA @ 1 FMB
        add_limit_order_internal<FMA, FMB>(owner, SIDE_BUY, 20000, 10000); // BUY 1 FMA @ 2 FMB
        let targetBuyID = add_limit_order_internal<FMA, FMB>(user2, SIDE_BUY, 100000, 10000); // BUY 1 FMA @ 10 FMB

        add_limit_order_internal<FMA, FMB>(owner, SIDE_SELL, 200000, 100000);  // SELL 10 FMA @ 20 FMB
        add_limit_order_internal<FMA, FMB>(owner, SIDE_SELL, 210000, 10000); // SELL 1 FMA @ 21 FMB
        add_limit_order_internal<FMA, FMB>(owner, SIDE_SELL, 250000, 10000); // SELL 1 FMA @ 25 FMB

        // SELL 1 FMA.
        let orderID = add_market_order_internal<FMA, FMB>(
            user1,
            SIDE_SELL,
            10000,
            0,
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

    #[test(owner = @ferum, user1 = @0x2, user2 = @0x3)]
    fun test_market_sell_execute_against_multiple_limits(owner: &signer, user1: &signer, user2: &signer) acquires OrderBook {
        // Tests that market sell order execute against multiple limit orders.

        account::create_account_for_test(address_of(owner));
        account::create_account_for_test(address_of(user1));
        account::create_account_for_test(address_of(user2));
        setup_fake_coins(owner, user1, 10000000000, 8);
        register_fma(owner, user2, 10000000000);
        register_fmb(owner, user2, 10000000000);
        setup_market_for_test<FMA, FMB>(owner);

        // Book setup.
        let targetBuyIDC = add_limit_order_internal<FMA, FMB>(user2, SIDE_BUY, 10000, 100000); // BUY 10 FMA @ 1 FMB
        let targetBuyIDB = add_limit_order_internal<FMA, FMB>(user2, SIDE_BUY, 20000, 10000); // BUY 1 FMA @ 2 FMB
        let targetBuyIDA = add_limit_order_internal<FMA, FMB>(user2, SIDE_BUY, 100000, 10000); // BUY 1 FMA @ 10 FMB

        add_limit_order_internal<FMA, FMB>(owner, SIDE_SELL, 200000, 100000);  // SELL 10 FMA @ 20 FMB
        add_limit_order_internal<FMA, FMB>(owner, SIDE_SELL, 210000, 10000); // SELL 1 FMA @ 21 FMB
        add_limit_order_internal<FMA, FMB>(owner, SIDE_SELL, 250000, 10000); // SELL 1 FMA @ 25 FMB

        // SELL 5 FMA.
        let orderID = add_market_order_internal<FMA, FMB>(
            user1,
            SIDE_SELL,
            50000,
            0,
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

    #[test(owner = @ferum, user1 = @0x2, user2 = @0x3)]
    fun test_market_buy_execute_against_multiple_limits(owner: &signer, user1: &signer, user2: &signer) acquires OrderBook {
        // Tests that market buy order execute against multiple limit orders.

        account::create_account_for_test(address_of(owner));
        account::create_account_for_test(address_of(user1));
        account::create_account_for_test(address_of(user2));
        setup_fake_coins(owner, user1, 50000000000, 8);
        register_fma(owner, user2, 50000000000);
        register_fmb(owner, user2, 50000000000);
        setup_market_for_test<FMA, FMB>(owner);

        // Book setup.
        add_limit_order_internal<FMA, FMB>(owner, SIDE_BUY, 10000, 100000); // BUY 10 FMA @ 1 FMB
        add_limit_order_internal<FMA, FMB>(owner, SIDE_BUY, 20000, 10000); // BUY 1 FMA @ 2 FMB
        add_limit_order_internal<FMA, FMB>(owner, SIDE_BUY, 100000, 10000); // BUY 1 FMA @ 10 FMB

        let targetSellIDA = add_limit_order_internal<FMA, FMB>(user2, SIDE_SELL, 200000, 100000);  // SELL 10 FMA @ 20 FMB
        let targetSellIDB = add_limit_order_internal<FMA, FMB>(user2, SIDE_SELL, 210000, 10000); // SELL 1 FMA @ 21 FMB
        let targetSellIDC = add_limit_order_internal<FMA, FMB>(user2, SIDE_SELL, 250000, 10000); // SELL 1 FMA @ 25 FMB

        // BUY 12 FMA spending at most 360 FMB.
        let orderID = add_market_order_internal<FMA, FMB>(
            user1,
            SIDE_BUY,
            120000,
            36000000000,
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

    #[test(owner = @ferum, user1 = @0x2, user2 = @0x3)]
    fun test_market_sell_eat_book_not_filled(owner: &signer, user1: &signer, user2: &signer) acquires OrderBook {
        // Tests that market sell order that eats through the book is cancelled.

        account::create_account_for_test(address_of(owner));
        account::create_account_for_test(address_of(user1));
        account::create_account_for_test(address_of(user2));
        setup_fake_coins(owner, user1, 50000000000, 8);
        register_fma(owner, user2, 50000000000);
        register_fmb(owner, user2, 50000000000);
        setup_market_for_test<FMA, FMB>(owner);

        // Book setup.
        let targetBuyIDA = add_limit_order_internal<FMA, FMB>(user2, SIDE_BUY, 100000, 10000); // BUY 1 FMA @ 10 FMB

        add_limit_order_internal<FMA, FMB>(owner, SIDE_SELL, 200000, 100000);  // SELL 10 FMA @ 20 FMB
        add_limit_order_internal<FMA, FMB>(owner, SIDE_SELL, 210000, 10000); // SELL 1 FMA @ 21 FMB
        add_limit_order_internal<FMA, FMB>(owner, SIDE_SELL, 250000, 10000); // SELL 1 FMA @ 25 FMB

        // SELL 2 FMA.
        let orderID = add_market_order_internal<FMA, FMB>(
            user1,
            SIDE_SELL,
            20000,
            0,
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

    #[test(owner = @ferum, user1 = @0x2, user2 = @0x3)]
    fun test_market_buy_eat_book_not_filled(owner: &signer, user1: &signer, user2: &signer) acquires OrderBook {
        // Tests that market buy order that eats through the book is cancelled.

        account::create_account_for_test(address_of(owner));
        account::create_account_for_test(address_of(user1));
        account::create_account_for_test(address_of(user2));
        setup_fake_coins(owner, user1, 50000000000, 8);
        register_fma(owner, user2, 50000000000);
        register_fmb(owner, user2, 50000000000);
        setup_market_for_test<FMA, FMB>(owner);

        // Book setup.
        add_limit_order_internal<FMA, FMB>(owner, SIDE_BUY, 10000, 100000); // BUY 10 FMA @ 1 FMB
        add_limit_order_internal<FMA, FMB>(owner, SIDE_BUY, 20000, 10000); // BUY 1 FMA @ 2 FMB
        add_limit_order_internal<FMA, FMB>(owner, SIDE_BUY, 100000, 10000); // BUY 1 FMA @ 10 FMB

        let targetSellIDA = add_limit_order_internal<FMA, FMB>(user2, SIDE_SELL, 250000, 10000); // SELL 1 FMA @ 25 FMB

        // BUY 2 FMA spending at most 360 FMB.
        let orderID = add_market_order_internal<FMA, FMB>(
            user1,
            SIDE_BUY,
            20000,
            36000000000,
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

    #[test(owner = @ferum, user1 = @0x2, user2 = @0x3)]
    fun test_limit_buy_execute(owner: &signer, user1: &signer, user2: &signer) acquires OrderBook {
        // Tests that limit buy order executes against other limit orders.

        account::create_account_for_test(address_of(owner));
        account::create_account_for_test(address_of(user1));
        account::create_account_for_test(address_of(user2));
        setup_fake_coins(owner, user1, 10000000000, 8);
        register_fma(owner, user2, 10000000000);
        register_fmb(owner, user2, 10000000000);
        setup_market_for_test<FMA, FMB>(owner);

        // Book setup.
        add_limit_order_internal<FMA, FMB>(owner, SIDE_BUY, 10000, 100000); // BUY 10 FMA @ 1 FMB
        add_limit_order_internal<FMA, FMB>(owner, SIDE_BUY, 20000, 10000); // BUY 1 FMA @ 2 FMB
        add_limit_order_internal<FMA, FMB>(owner, SIDE_BUY, 100000, 10000); // BUY 1 FMA @ 10 FMB

        let targetSellID = add_limit_order_internal<FMA, FMB>(user2, SIDE_SELL, 200000, 100000); // SELL 10 FMA @ 20 FMB
        add_limit_order_internal<FMA, FMB>(owner, SIDE_SELL, 210000, 10000); // SELL 1 FMA @ 21 FMB
        add_limit_order_internal<FMA, FMB>(owner, SIDE_SELL, 250000, 10000); // SELL 1 FMA @ 25 FMB

        // BUY 1 FMA @ 20 FMB.
        let orderID = add_limit_order_internal<FMA, FMB>(
            user1,
            SIDE_BUY,
            200000,
            10000,
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

    #[test(owner = @ferum, user1 = @0x2, user2 = @0x3)]
    fun test_limit_sell_execute(owner: &signer, user1: &signer, user2: &signer) acquires OrderBook {
        // Tests that limit sell order executes against other limit orders.

        account::create_account_for_test(address_of(owner));
        account::create_account_for_test(address_of(user1));
        account::create_account_for_test(address_of(user2));
        setup_fake_coins(owner, user1, 10000000000, 8);
        register_fma(owner, user2, 10000000000);
        register_fmb(owner, user2, 10000000000);
        setup_market_for_test<FMA, FMB>(owner);

        // Book setup.
        add_limit_order_internal<FMA, FMB>(owner, SIDE_BUY, 10000, 100000); // BUY 10 FMA @ 1 FMB
        add_limit_order_internal<FMA, FMB>(owner, SIDE_BUY, 95000, 10000); // BUY 1 FMA @ 9.5 FMB
        let targetBuyID = add_limit_order_internal<FMA, FMB>(user2, SIDE_BUY, 100000, 10000); // BUY 1 FMA @ 10 FMB

        add_limit_order_internal<FMA, FMB>(owner, SIDE_SELL, 200000, 100000); // SELL 10 FMA @ 20 FMB
        add_limit_order_internal<FMA, FMB>(owner, SIDE_SELL, 210000, 10000); // SELL 1 FMA @ 21 FMB
        add_limit_order_internal<FMA, FMB>(owner, SIDE_SELL, 250000, 10000); // SELL 1 FMA @ 25 FMB

        // SELL 1 FMA @ 9 FMB.
        let orderID = add_limit_order_internal<FMA, FMB>(
            user1,
            SIDE_SELL,
            90000,
            10000,
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

    #[test(owner = @ferum, user1 = @0x2, user2 = @0x3)]
    fun test_limit_buy_execute_multiple(owner: &signer, user1: &signer, user2: &signer) acquires OrderBook {
        // Tests that limit buy order executes against multiple other limit orders.

        account::create_account_for_test(address_of(owner));
        account::create_account_for_test(address_of(user1));
        account::create_account_for_test(address_of(user2));
        setup_fake_coins(owner, user1, 50000000000, 8);
        register_fma(owner, user2, 50000000000);
        register_fmb(owner, user2, 50000000000);
        setup_market_for_test<FMA, FMB>(owner);

        // Book setup.
        add_limit_order_internal<FMA, FMB>(owner, SIDE_BUY, 10000, 100000); // BUY 10 FMA @ 1 FMB
        add_limit_order_internal<FMA, FMB>(owner, SIDE_BUY, 20000, 10000); // BUY 1 FMA @ 2 FMB
        add_limit_order_internal<FMA, FMB>(owner, SIDE_BUY, 100000, 10000); // BUY 1 FMA @ 10 FMB

        let targetSellIDA = add_limit_order_internal<FMA, FMB>(user2, SIDE_SELL, 200000, 100000); // SELL 10 FMA @ 20 FMB
        let targetSellIDB = add_limit_order_internal<FMA, FMB>(user2, SIDE_SELL, 210000, 10000); // SELL 1 FMA @ 21 FMB
        let targetSellIDC = add_limit_order_internal<FMA, FMB>(user2, SIDE_SELL, 250000, 10000); // SELL 1 FMA @ 25 FMB

        // BUY 11 FMA @ 22 FMB.
        let orderID = add_limit_order_internal<FMA, FMB>(
            user1,
            SIDE_BUY,
            220000,
            110000,
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

    #[test(owner = @ferum, user1 = @0x2, user2 = @0x3)]
    fun test_limit_sell_execute_multiple(owner: &signer, user1: &signer, user2: &signer) acquires OrderBook {
        // Tests that limit sell order executes against multiple other limit orders.

        account::create_account_for_test(address_of(owner));
        account::create_account_for_test(address_of(user1));
        account::create_account_for_test(address_of(user2));
        setup_fake_coins(owner, user1, 50000000000, 8);
        register_fma(owner, user2, 50000000000);
        register_fmb(owner, user2, 50000000000);
        setup_market_for_test<FMA, FMB>(owner);

        // Book setup.
        let targetBuyIDC = add_limit_order_internal<FMA, FMB>(user2, SIDE_BUY, 10000, 100000); // BUY 10 FMA @ 1 FMB
        let targetBuyIDB = add_limit_order_internal<FMA, FMB>(user2, SIDE_BUY, 20000, 10000); // BUY 1 FMA @ 2 FMB
        let targetBuyIDA = add_limit_order_internal<FMA, FMB>(user2, SIDE_BUY, 100000, 10000); // BUY 1 FMA @ 10 FMB

        add_limit_order_internal<FMA, FMB>(owner, SIDE_SELL, 200000, 100000); // SELL 10 FMA @ 20 FMB
        add_limit_order_internal<FMA, FMB>(owner, SIDE_SELL, 210000, 10000); // SELL 1 FMA @ 21 FMB
        add_limit_order_internal<FMA, FMB>(owner, SIDE_SELL, 250000, 10000); // SELL 1 FMA @ 25 FMB

        // SELL 11 FMA @ 1.5 FMB.
        let orderID = add_limit_order_internal<FMA, FMB>(
            user1,
            SIDE_SELL,
            15000,
            110000,
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

    #[test(owner = @ferum, user1 = @0x2, user2 = @0x3)]
    fun test_limit_orders_precision(owner: &signer, user1: &signer, user2: &signer) acquires OrderBook {
        // Tests that limit order executions that require more precision than the market's instrumentDecimals or
        // quoteDecimals don't fail (because both parameters are set so that they can be multiplied without exceeding
        // the underlying coin's decimals).

        account::create_account_for_test(address_of(owner));
        account::create_account_for_test(address_of(user1));
        account::create_account_for_test(address_of(user2));
        setup_fake_coins(owner, user1, 50000000000, 8);
        register_fma(owner, user2, 50000000000);
        register_fmb(owner, user2, 50000000000);
        setup_market_for_test<FMA, FMB>(owner);

        // Book setup.
        let buyID = add_limit_order_internal<FMA, FMB>(user2, SIDE_BUY, 2, 2); // BUY 0.0002 FMA @ 0.0002 FMB
        let sellID = add_limit_order_internal<FMA, FMB>(user1, SIDE_SELL, 1, 1); // SELL 0.0001 FMA @ 0.0001 FMB

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

    #[test_only]
    fun setup_market_for_test<I, Q>(owner: &signer) {
        init_ferum(owner);
        init_market<I, Q>(owner, 4, 4);
    }

    #[test_only]
    fun get_order<I, Q>(book: &OrderBook<I, Q>, orderID: u128): &Order<I, Q> {
        let orderMap = &book.orderMap;
        let contains = table::contains(orderMap, orderID);

        if (contains) {
            table::borrow(orderMap, orderID)
        } else {
            table::borrow(&book.finalizedOrderMap, orderID)
        }
    }
}
```
