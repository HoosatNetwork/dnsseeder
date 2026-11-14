package main

import (
	"net"
	"testing"
	"time"

	"github.com/Hoosat-Oy/HTND/app/appmessage"
	"github.com/Hoosat-Oy/HTND/infrastructure/config"
)

func newTestManager(t *testing.T) *Manager {
	activeConfig = &ConfigFlags{NetworkFlags: config.NetworkFlags{}}
	if err := activeConfig.NetworkFlags.ResolveNetwork(nil); err != nil {
		t.Fatalf("ResolveNetwork: %s", err)
	}
	peersDefaultPort = 42421
	return &Manager{nodes: make(map[string]*Node), failed: make(map[string]time.Time)}
}

func testAddr(i int, port int) *appmessage.NetAddress {
	return appmessage.NewNetAddressIPPort(net.IPv4(8, 8, byte(i>>8), byte(i)), uint16(port))
}

func TestAddAddressesDefaultPortOnly(t *testing.T) {
	m := newTestManager(t)
	added := m.AddAddresses([]*appmessage.NetAddress{testAddr(1, peersDefaultPort), testAddr(2, 16111)})
	if added != 1 || m.AddressCount() != 1 {
		t.Fatalf("added %d, count %d; want 1, 1", added, m.AddressCount())
	}
	// the same IP on another port must not shadow it
	m.AddAddresses([]*appmessage.NetAddress{testAddr(3, 16111)})
	if m.AddAddresses([]*appmessage.NetAddress{testAddr(3, peersDefaultPort)}) != 1 {
		t.Fatalf("default-port address was shadowed by another port")
	}
}

func TestFailedAddressCooldown(t *testing.T) {
	m := newTestManager(t)
	a := testAddr(1, peersDefaultPort)
	m.AddAddresses([]*appmessage.NetAddress{a})
	m.Attempt(a.IP)
	m.prunePeers()
	if m.AddressCount() != 0 {
		t.Fatalf("failed address not pruned")
	}
	if m.AddAddresses([]*appmessage.NetAddress{a}) != 0 {
		t.Fatalf("failed address re-added during cooldown")
	}
	m.failed[a.IP.String()] = time.Now().Add(-failedCooldown)
	m.prunePeers()
	if len(m.failed) != 0 {
		t.Fatalf("expired cooldown entry kept")
	}
	if m.AddAddresses([]*appmessage.NetAddress{a}) != 1 {
		t.Fatalf("address not accepted after cooldown")
	}
	// a node that answered once is not put on cooldown
	b := testAddr(2, peersDefaultPort)
	m.AddAddresses([]*appmessage.NetAddress{b})
	m.Good(b.IP, nil)
	m.Attempt(b.IP)
	m.prunePeers()
	if _, ok := m.failed[b.IP.String()]; ok {
		t.Fatalf("good node put on cooldown")
	}
}

func TestAddressesRechecksGoodNodesFirst(t *testing.T) {
	m := newTestManager(t)
	for i := 0; i < 200; i++ {
		m.AddAddresses([]*appmessage.NetAddress{testAddr(i, peersDefaultPort)})
	}
	due, fresh := testAddr(1000, peersDefaultPort), testAddr(1001, peersDefaultPort)
	m.AddAddresses([]*appmessage.NetAddress{due, fresh})
	checked := time.Now().Add(-50 * time.Minute)
	m.nodes[due.IP.String()].LastSuccess = checked
	m.nodes[due.IP.String()].LastAttempt = checked
	m.Good(fresh.IP, nil)
	m.Attempt(fresh.IP)

	for round := 0; round < 20; round++ {
		addrs := m.Addresses()
		if len(addrs) != 1+maxUncheckedPerRound {
			t.Fatalf("got %d addresses, want %d", len(addrs), 1+maxUncheckedPerRound)
		}
		if !addrs[0].IP.Equal(due.IP) {
			t.Fatalf("good node due for a re-check not polled first")
		}
		for _, a := range addrs {
			if a.IP.Equal(fresh.IP) {
				t.Fatalf("node checked just now picked for a re-check")
			}
		}
	}
}

func TestPruneDropsWrongPortWithoutCooldown(t *testing.T) {
	m := newTestManager(t)
	// an entry from an older address book: same IP, another port, already tried
	legacy := testAddr(1, 16111)
	m.nodes[legacy.IP.String()] = &Node{Addr: legacy, LastSeen: time.Now(), LastAttempt: time.Now()}
	m.prunePeers()
	if m.AddressCount() != 0 {
		t.Fatalf("wrong-port entry not pruned")
	}
	if m.AddAddresses([]*appmessage.NetAddress{testAddr(1, peersDefaultPort)}) != 1 {
		t.Fatalf("default-port address blocked by a pruned wrong-port entry")
	}
}
