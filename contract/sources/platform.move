module ferum::platform {
    use std::signer::address_of;

    const ERR_CUSTODIAN_ALREADY_REGISTERED: u64 = 1;
    const ERR_INVALID_CUSTODIAN_ADDRESS: u64 = 2;
    const ERR_PROTOCOL_ALREADY_REGISTERED: u64 = 3;
    const ERR_INVALID_PROTOCOL_ADDRESS: u64 = 4;

    // Capability used to identify order placement through a Ferum protocol.
    struct ProtocolCapability has store {
        protocolAddress: address, // 0x0 is reserved as the sentinal value.
    }

    // Used to identify user that placed the order from a given protocol.
    struct UserIdentifier has store, drop, copy {
        protocolAddress: address, // 0x0 is reserved as the sentinal value.
        userAddress: address, // 0x0 is reserved as the sentinal value.
    }

    // Struct used to store information about a protocol.
    struct ProtocolInfo has key {}

    //
    // Public functions.
    //

    public fun is_address_valid(addr: address): bool {
        return addr != @0x0
    }

    public fun register_protocol(owner: &signer): ProtocolCapability {
        let ownerAddr = address_of(owner);
        assert!(!exists<ProtocolInfo>(ownerAddr), ERR_CUSTODIAN_ALREADY_REGISTERED);
        assert!(is_address_valid(ownerAddr), ERR_INVALID_CUSTODIAN_ADDRESS);
        move_to(owner, ProtocolInfo{});

        ProtocolCapability{
            protocolAddress: ownerAddr,
        }
    }

    public fun get_protocol_address(cap: &ProtocolCapability): address {
        return cap.protocolAddress
    }

    public fun sentinal_user_identifier(): UserIdentifier {
        UserIdentifier {
            protocolAddress: @0x0,
            userAddress: @0x0,
        }
    }

    public fun is_user_identifier_valid(identifier: &UserIdentifier): bool {
        is_address_valid(identifier.protocolAddress) && is_address_valid(identifier.userAddress)
    }

    public fun get_user_identifier(user: &signer, protocolCap: &ProtocolCapability): UserIdentifier {
        let userAddress = address_of(user);
        let protocolAddress = get_protocol_address(protocolCap);

        UserIdentifier {
            userAddress,
            protocolAddress,
        }
    }

    // Returns (protocolAddress, userAddress)
    public fun get_addresses_from_user_identifier(id: &UserIdentifier): (address, address) {
        (id.protocolAddress, id.userAddress)
    }

    #[test_only]
    public fun drop_protocol_capability(cap: ProtocolCapability) {
        let ProtocolCapability {protocolAddress: _} = cap;
    }
}