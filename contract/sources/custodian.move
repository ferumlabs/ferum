module ferum::custodian {
    use std::signer::address_of;

    const ERR_CUSTODIAN_ALREADY_REGISTERED: u64 = 1;
    const ERR_INVALID_CUSTODIAN_ADDRESS: u64 = 2;

    // Capability used to assign custodianship to a third party.
    struct CustodianCapability has store {
        custodianAddress: address, // 0x0 is reserved as the sentinal value.
    }

    // Struct used to store information about a custodian.
    struct CustodianInfo has key {}

    //
    // Public functions.
    //

    public fun register_custodian(owner: &signer): CustodianCapability {
        let ownerAddr = address_of(owner);
        assert!(!exists<CustodianInfo>(ownerAddr), ERR_CUSTODIAN_ALREADY_REGISTERED);
        assert!(is_custodian_address_valid(ownerAddr), ERR_INVALID_CUSTODIAN_ADDRESS);
        move_to(owner, CustodianInfo{});

        CustodianCapability{
            custodianAddress: ownerAddr,
        }
    }

    public fun get_custodian_address(cap: &CustodianCapability): address {
        return cap.custodianAddress
    }

    public fun is_custodian_address_valid(addr: address): bool {
        return addr != @0x0
    }

    #[test_only]
    public fun drop_custodian_capability(cap: CustodianCapability) {
        let CustodianCapability {custodianAddress: _} = cap;
    }
}