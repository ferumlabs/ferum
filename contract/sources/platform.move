module ferum::platform {
    use std::signer::address_of;

    friend ferum::market;

    //
    // Errors
    //

    const ERR_PROTOCOL_ALREADY_REGISTERED: u64 = 600;
    const ERR_INVALID_PROTOCOL_ADDRESS: u64 = 601;

    // Capability used to identify order placement through a Ferum protocol.
    struct ProtocolCapability has store {
        protocolAddress: address, // 0x0 is reserved as the sentinal value.
    }

    // Used to identify user that placed the order from a given protocol.
    // Should only be able to be generated using a ProtocolCapability.
    struct AccountIdentifier has drop {
        protocolAddress: address, // 0x0 is reserved as the sentinal value.
        userAddress: address, // 0x0 is reserved as the sentinal value.
    }

    // Struct used to store information about a protocol.
    struct ProtocolInfo has key {}

    public fun register_protocol(owner: &signer): ProtocolCapability {
        let ownerAddr = address_of(owner);
        assert!(!exists<ProtocolInfo>(ownerAddr), ERR_PROTOCOL_ALREADY_REGISTERED);
        assert!(is_address_valid(ownerAddr), ERR_INVALID_PROTOCOL_ADDRESS);
        move_to(owner, ProtocolInfo{});

        ProtocolCapability{
            protocolAddress: ownerAddr,
        }
    }

    public fun gen_account_identifier(user: &signer, protocolCap: &ProtocolCapability): AccountIdentifier {
        let userAddress = address_of(user);
        let protocolAddress = get_protocol_address(protocolCap);
        AccountIdentifier {
            userAddress,
            protocolAddress,
        }
    }

    public(friend) fun is_address_valid(addr: address): bool {
        return addr != @0x0
    }

    public(friend) fun is_account_identifier_valid(identifier: &AccountIdentifier): bool {
        is_address_valid(identifier.protocolAddress) && is_address_valid(identifier.userAddress)
    }

    // Returns (protocolAddress, userAddress)
    public(friend) fun get_addresses(id: &AccountIdentifier): (address, address) {
        (id.protocolAddress, id.userAddress)
    }

    public(friend) fun get_protocol_address(cap: &ProtocolCapability): address {
        return cap.protocolAddress
    }

    public(friend) fun get_user_address(identifier: &AccountIdentifier): address {
        return identifier.userAddress
    }

    #[test_only]
    public fun account_identifier_for_test(user: &signer): AccountIdentifier {
        let userAddress = address_of(user);
        AccountIdentifier {
            userAddress,
            protocolAddress: @ferum,
        }
    }

    #[test_only]
    public fun drop_protocol_capability(cap: ProtocolCapability) {
        let ProtocolCapability {protocolAddress: _} = cap;
    }
}