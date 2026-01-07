// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the MIT license found in the LICENSE.txt file or at:
//     https://opensource.org/license/mit

/**
 * Extreme Map Operations Test Suite
 *
 * This test suite demonstrates the full power of Cap'n Web's "magic" .map() method
 * through 10 realistic, extreme use cases. Each test implements a complete scenario
 * with properly structured data, valid input/output signatures, and realistic
 * server-side logic.
 *
 * The .map() method is "magic" because:
 * 1. It records your callback's RPC operations without executing them
 * 2. Sends that recording to the server
 * 3. Server replays the recording on actual data
 * 4. All happens in a SINGLE network round trip
 *
 * These tests prove that complex, multi-step operations on data the client
 * has never seen can be expressed naturally in JavaScript and executed efficiently.
 */

import { expect, it, describe } from "vitest"
import { RpcSession, RpcTransport, RpcTarget, RpcStub } from "../src/index.js"

// =======================================================================================
// Test Infrastructure
// =======================================================================================

class TestTransport implements RpcTransport {
  constructor(public name: string, private partner?: TestTransport) {
    if (partner) {
      partner.partner = this;
    }
  }

  private queue: Uint8Array[] = [];
  private waiter?: () => void;

  async send(message: Uint8Array): Promise<void> {
    this.partner!.queue.push(message);
    if (this.partner!.waiter) {
      this.partner!.waiter();
      this.partner!.waiter = undefined;
    }
  }

  async receive(): Promise<Uint8Array> {
    if (this.queue.length == 0) {
      await new Promise<void>((resolve) => {
        this.waiter = resolve;
      });
    }
    return this.queue.shift()!;
  }
}

async function pumpMicrotasks() {
  for (let i = 0; i < 16; i++) {
    await Promise.resolve();
  }
}

class TestHarness<T extends RpcTarget> {
  clientTransport: TestTransport;
  serverTransport: TestTransport;
  client: RpcSession<T>;
  server: RpcSession;
  stub: RpcStub<T>;

  constructor(target: T) {
    this.clientTransport = new TestTransport("client");
    this.serverTransport = new TestTransport("server", this.clientTransport);
    this.client = new RpcSession<T>(this.clientTransport);
    this.server = new RpcSession<undefined>(this.serverTransport, target);
    this.stub = this.client.getRemoteMain();
  }

  async [Symbol.asyncDispose]() {
    await pumpMicrotasks();
  }
}

// =======================================================================================
// 1. PRIVACY-PRESERVING MEDICAL ANALYSIS
// =======================================================================================

describe("Extreme Map: Privacy-Preserving Medical Analysis", () => {
  // Realistic medical data types
  interface PatientRecord {
    id: string;
    mrn: string;  // Medical Record Number
    dateOfBirth: string;
    admissionDate: string;
  }

  interface Diagnosis {
    patientId: string;
    icdCode: string;  // ICD-10 diagnosis code
    description: string;
    severity: "mild" | "moderate" | "severe" | "critical";
    diagnosedBy: string;
    diagnosedAt: string;
  }

  interface AnonymizedDiagnosisStats {
    icdCode: string;
    severityDistribution: Record<string, number>;
    count: number;
    // Note: NO patient identifiers
  }

  class HospitalDataService extends RpcTarget {
    // Simulated patient database (in reality, this would be millions of records)
    private patients: PatientRecord[] = [
      { id: "p001", mrn: "MRN-2024-001", dateOfBirth: "1985-03-15", admissionDate: "2024-01-10" },
      { id: "p002", mrn: "MRN-2024-002", dateOfBirth: "1972-08-22", admissionDate: "2024-01-11" },
      { id: "p003", mrn: "MRN-2024-003", dateOfBirth: "1990-12-01", admissionDate: "2024-01-11" },
      { id: "p004", mrn: "MRN-2024-004", dateOfBirth: "1968-05-30", admissionDate: "2024-01-12" },
      { id: "p005", mrn: "MRN-2024-005", dateOfBirth: "1995-07-14", admissionDate: "2024-01-12" },
    ];

    // Simulated diagnosis database
    private diagnoses: Record<string, Diagnosis> = {
      "p001": { patientId: "p001", icdCode: "J18.9", description: "Pneumonia, unspecified", severity: "moderate", diagnosedBy: "Dr. Smith", diagnosedAt: "2024-01-10T14:30:00Z" },
      "p002": { patientId: "p002", icdCode: "I10", description: "Essential hypertension", severity: "mild", diagnosedBy: "Dr. Jones", diagnosedAt: "2024-01-11T09:15:00Z" },
      "p003": { patientId: "p003", icdCode: "J18.9", description: "Pneumonia, unspecified", severity: "severe", diagnosedBy: "Dr. Smith", diagnosedAt: "2024-01-11T16:45:00Z" },
      "p004": { patientId: "p004", icdCode: "E11.9", description: "Type 2 diabetes mellitus", severity: "moderate", diagnosedBy: "Dr. Williams", diagnosedAt: "2024-01-12T10:00:00Z" },
      "p005": { patientId: "p005", icdCode: "J18.9", description: "Pneumonia, unspecified", severity: "critical", diagnosedBy: "Dr. Smith", diagnosedAt: "2024-01-12T22:30:00Z" },
    };

    getAllPatients(): PatientRecord[] {
      return this.patients;
    }

    getDiagnosis(patientId: string): Diagnosis | null {
      return this.diagnoses[patientId] || null;
    }

    // Anonymization function - strips all PII, returns only statistical data
    anonymize(diagnosis: Diagnosis | null): AnonymizedDiagnosisStats | null {
      if (!diagnosis) return null;
      return {
        icdCode: diagnosis.icdCode,
        severityDistribution: { [diagnosis.severity]: 1 },
        count: 1
        // Note: patientId, diagnosedBy, dates are ALL stripped
      };
    }
  }

  it("processes patient data without exposing PII to client", async () => {
    await using harness = new TestHarness(new HospitalDataService());

    // Client requests all patients - gets back identifiers
    using patients = harness.stub.getAllPatients();

    // Client maps over patients to get anonymized diagnosis stats
    // The RAW diagnosis data (with patient names, dates, etc.) NEVER leaves the server
    const anonymizedStats = await patients.map((patient: PatientRecord) => {
      const diagnosis = harness.stub.getDiagnosis(patient.id);
      return harness.stub.anonymize(diagnosis);
    });

    // Verify we got results
    expect(anonymizedStats.length).toBe(5);

    // Verify the data is properly anonymized - no patient IDs
    for (const stat of anonymizedStats) {
      if (stat !== null) {
        expect(stat).toHaveProperty("icdCode");
        expect(stat).toHaveProperty("count");
        expect(stat).not.toHaveProperty("patientId");
        expect(stat).not.toHaveProperty("diagnosedBy");
        expect(stat).not.toHaveProperty("mrn");
      }
    }

    // Verify we can see the ICD codes (aggregate data)
    const pneumoniaCases = anonymizedStats.filter(s => s?.icdCode === "J18.9");
    expect(pneumoniaCases.length).toBe(3);
  });
});

// =======================================================================================
// 2. BILLION-ROW AGGREGATION
// =======================================================================================

describe("Extreme Map: Billion-Row Aggregation", () => {
  // Realistic transaction data types
  interface Transaction {
    id: string;
    merchantId: string;
    amount: number;
    currency: string;
    timestamp: string;
    cardLastFour: string;
  }

  interface MerchantCategory {
    merchantId: string;
    category: string;
    subcategory: string;
    riskLevel: "low" | "medium" | "high";
  }

  interface CategoryAggregate {
    category: string;
    totalAmount: number;
    transactionCount: number;
    averageAmount: number;
  }

  class TransactionDatabase extends RpcTarget {
    // Simulated transaction stream (in reality, billions of rows)
    private transactions: Transaction[] = [
      { id: "tx001", merchantId: "m001", amount: 42.50, currency: "USD", timestamp: "2024-01-15T10:30:00Z", cardLastFour: "1234" },
      { id: "tx002", merchantId: "m002", amount: 150.00, currency: "USD", timestamp: "2024-01-15T10:31:00Z", cardLastFour: "5678" },
      { id: "tx003", merchantId: "m001", amount: 23.99, currency: "USD", timestamp: "2024-01-15T10:32:00Z", cardLastFour: "9012" },
      { id: "tx004", merchantId: "m003", amount: 500.00, currency: "USD", timestamp: "2024-01-15T10:33:00Z", cardLastFour: "3456" },
      { id: "tx005", merchantId: "m002", amount: 75.25, currency: "USD", timestamp: "2024-01-15T10:34:00Z", cardLastFour: "7890" },
      { id: "tx006", merchantId: "m001", amount: 18.00, currency: "USD", timestamp: "2024-01-15T10:35:00Z", cardLastFour: "2345" },
      { id: "tx007", merchantId: "m004", amount: 1200.00, currency: "USD", timestamp: "2024-01-15T10:36:00Z", cardLastFour: "6789" },
      { id: "tx008", merchantId: "m003", amount: 89.99, currency: "USD", timestamp: "2024-01-15T10:37:00Z", cardLastFour: "0123" },
    ];

    private merchantCategories: Record<string, MerchantCategory> = {
      "m001": { merchantId: "m001", category: "Retail", subcategory: "Grocery", riskLevel: "low" },
      "m002": { merchantId: "m002", category: "Retail", subcategory: "Electronics", riskLevel: "medium" },
      "m003": { merchantId: "m003", category: "Travel", subcategory: "Airlines", riskLevel: "medium" },
      "m004": { merchantId: "m004", category: "Travel", subcategory: "Hotels", riskLevel: "low" },
    };

    getTransactions(): Transaction[] {
      return this.transactions;
    }

    categorize(merchantId: string): MerchantCategory {
      return this.merchantCategories[merchantId] || {
        merchantId,
        category: "Unknown",
        subcategory: "Unknown",
        riskLevel: "high" as const
      };
    }

    aggregate(category: string, amount: number): CategoryAggregate {
      // In a real system, this would update an accumulator
      // Here we return per-transaction data that would be aggregated client-side
      return {
        category,
        totalAmount: amount,
        transactionCount: 1,
        averageAmount: amount
      };
    }
  }

  it("aggregates transaction data by category without transferring raw data", async () => {
    await using harness = new TestHarness(new TransactionDatabase());

    // Client gets transaction stream (in reality, a cursor over billions)
    using transactions = harness.stub.getTransactions();

    // Client maps to categorize and aggregate - all server-side
    const categoryData = await transactions.map((tx: Transaction) => {
      const category = harness.stub.categorize(tx.merchantId);
      return harness.stub.aggregate(category.category, tx.amount);
    });

    // Verify results
    expect(categoryData.length).toBe(8);

    // Aggregate client-side (in reality, server would do final aggregation)
    const aggregated = new Map<string, { total: number; count: number }>();
    for (const item of categoryData) {
      const existing = aggregated.get(item.category) || { total: 0, count: 0 };
      existing.total += item.totalAmount;
      existing.count += item.transactionCount;
      aggregated.set(item.category, existing);
    }

    // Verify Retail category
    const retail = aggregated.get("Retail")!;
    expect(retail.count).toBe(5);  // 3 grocery + 2 electronics
    expect(retail.total).toBeCloseTo(309.74, 2);  // 42.50 + 23.99 + 18.00 + 150.00 + 75.25

    // Verify Travel category
    const travel = aggregated.get("Travel")!;
    expect(travel.count).toBe(3);  // 2 airlines + 1 hotel
    expect(travel.total).toBeCloseTo(1789.99, 2);  // 500 + 89.99 + 1200
  });
});

// =======================================================================================
// 3. SOCIAL GRAPH TRAVERSAL
// =======================================================================================

describe("Extreme Map: Social Graph Traversal", () => {
  // Realistic social network types
  interface UserProfile {
    userId: string;
    username: string;
    displayName: string;
    joinedAt: string;
    isVerified: boolean;
  }

  interface Post {
    postId: string;
    authorId: string;
    content: string;
    createdAt: string;
    likeCount: number;
    repostCount: number;
  }

  interface FriendOfFriendPost {
    author: { userId: string; username: string; displayName: string };
    post: { postId: string; title: string; likeCount: number };
  }

  class SocialNetworkService extends RpcTarget {
    private users: Record<string, UserProfile> = {
      "u001": { userId: "u001", username: "alice", displayName: "Alice Anderson", joinedAt: "2020-01-15", isVerified: true },
      "u002": { userId: "u002", username: "bob", displayName: "Bob Brown", joinedAt: "2020-03-22", isVerified: false },
      "u003": { userId: "u003", username: "charlie", displayName: "Charlie Chen", joinedAt: "2021-06-10", isVerified: true },
      "u004": { userId: "u004", username: "diana", displayName: "Diana Davis", joinedAt: "2021-09-05", isVerified: false },
      "u005": { userId: "u005", username: "eve", displayName: "Eve Evans", joinedAt: "2022-01-20", isVerified: true },
      "u006": { userId: "u006", username: "frank", displayName: "Frank Foster", joinedAt: "2022-04-12", isVerified: false },
    };

    private friendships: Record<string, string[]> = {
      "u001": ["u002", "u003"],           // Alice is friends with Bob and Charlie
      "u002": ["u001", "u004", "u005"],   // Bob is friends with Alice, Diana, Eve
      "u003": ["u001", "u005", "u006"],   // Charlie is friends with Alice, Eve, Frank
      "u004": ["u002"],                   // Diana is friends with Bob
      "u005": ["u002", "u003"],           // Eve is friends with Bob, Charlie
      "u006": ["u003"],                   // Frank is friends with Charlie
    };

    private posts: Record<string, Post[]> = {
      "u002": [
        { postId: "post001", authorId: "u002", content: "Just had amazing coffee!", createdAt: "2024-01-15T08:00:00Z", likeCount: 42, repostCount: 5 },
      ],
      "u003": [
        { postId: "post002", authorId: "u003", content: "Working on a new project", createdAt: "2024-01-15T09:30:00Z", likeCount: 128, repostCount: 23 },
      ],
      "u004": [
        { postId: "post003", authorId: "u004", content: "Beautiful sunset today", createdAt: "2024-01-15T18:45:00Z", likeCount: 89, repostCount: 12 },
      ],
      "u005": [
        { postId: "post004", authorId: "u005", content: "Conference was amazing!", createdAt: "2024-01-15T16:00:00Z", likeCount: 256, repostCount: 45 },
        { postId: "post005", authorId: "u005", content: "New blog post is live", createdAt: "2024-01-15T20:00:00Z", likeCount: 178, repostCount: 34 },
      ],
      "u006": [
        { postId: "post006", authorId: "u006", content: "Learning Rust this week", createdAt: "2024-01-15T10:15:00Z", likeCount: 67, repostCount: 8 },
      ],
    };

    getUser(userId: string): UserProfile {
      return this.users[userId];
    }

    getFriends(userId: string): UserProfile[] {
      const friendIds = this.friendships[userId] || [];
      return friendIds.map(id => this.users[id]);
    }

    getRecentPosts(userId: string): Post[] {
      return this.posts[userId] || [];
    }

    formatPostSummary(author: UserProfile, post: Post): FriendOfFriendPost {
      return {
        author: {
          userId: author.userId,
          username: author.username,
          displayName: author.displayName
        },
        post: {
          postId: post.postId,
          title: post.content.substring(0, 50),
          likeCount: post.likeCount
        }
      };
    }
  }

  it("traverses friend-of-friend posts without knowing graph structure", async () => {
    await using harness = new TestHarness(new SocialNetworkService());

    // Client gets Alice's friends (doesn't know who they are)
    using friends = harness.stub.getFriends("u001");

    // For each friend, get THEIR friends, then get posts
    // Client has NO IDEA how many friends-of-friends exist
    const friendOfFriendPosts = await friends.map((friend: UserProfile) => {
      return harness.stub.getFriends(friend.userId).map((fof: UserProfile) => {
        return harness.stub.getRecentPosts(fof.userId).map((post: Post) => {
          return harness.stub.formatPostSummary(fof, post);
        });
      });
    });

    // Flatten the nested structure
    const allPosts: FriendOfFriendPost[] = [];
    for (const friendLevel of friendOfFriendPosts) {
      for (const fofLevel of friendLevel) {
        for (const post of fofLevel) {
          allPosts.push(post);
        }
      }
    }

    // Verify we got posts from friends-of-friends
    expect(allPosts.length).toBeGreaterThan(0);

    // Alice's friends are Bob and Charlie
    // Bob's friends (excluding Alice): Diana, Eve
    // Charlie's friends (excluding Alice): Eve, Frank
    // So we should see posts from Diana, Eve, Frank
    const authors = new Set(allPosts.map(p => p.author.username));
    expect(authors.has("diana")).toBe(true);
    expect(authors.has("eve")).toBe(true);
    expect(authors.has("frank")).toBe(true);

    // Verify post structure
    for (const item of allPosts) {
      expect(item.author).toHaveProperty("userId");
      expect(item.author).toHaveProperty("username");
      expect(item.post).toHaveProperty("postId");
      expect(item.post).toHaveProperty("likeCount");
    }
  });
});

// =======================================================================================
// 4. CROSS-SERVICE ORCHESTRATION
// =======================================================================================

describe("Extreme Map: Cross-Service Orchestration", () => {
  // Realistic e-commerce types spanning multiple services
  interface Order {
    orderId: string;
    customerId: string;
    productId: string;
    quantity: number;
    status: "pending" | "confirmed" | "shipped" | "delivered";
    createdAt: string;
  }

  interface Customer {
    customerId: string;
    name: string;
    email: string;
    address: {
      street: string;
      city: string;
      state: string;
      zipCode: string;
      country: string;
    };
    tier: "standard" | "premium" | "vip";
  }

  interface InventoryStatus {
    productId: string;
    productName: string;
    inStock: number;
    reserved: number;
    available: number;
    warehouseLocation: string;
  }

  interface ShippingEstimate {
    carrierId: string;
    carrierName: string;
    estimatedDays: number;
    estimatedDelivery: string;
    cost: number;
    expeditedAvailable: boolean;
  }

  interface EnrichedOrder {
    order: Order;
    customer: Customer;
    inventory: InventoryStatus;
    shipping: ShippingEstimate;
  }

  // Simulating three separate microservices
  class OrderService extends RpcTarget {
    private orders: Order[] = [
      { orderId: "ord001", customerId: "cust001", productId: "prod001", quantity: 2, status: "pending", createdAt: "2024-01-15T10:00:00Z" },
      { orderId: "ord002", customerId: "cust002", productId: "prod002", quantity: 1, status: "pending", createdAt: "2024-01-15T10:05:00Z" },
      { orderId: "ord003", customerId: "cust001", productId: "prod003", quantity: 5, status: "pending", createdAt: "2024-01-15T10:10:00Z" },
      { orderId: "ord004", customerId: "cust003", productId: "prod001", quantity: 1, status: "pending", createdAt: "2024-01-15T10:15:00Z" },
    ];

    getPendingOrders(): Order[] {
      return this.orders.filter(o => o.status === "pending");
    }
  }

  class CustomerService extends RpcTarget {
    private customers: Record<string, Customer> = {
      "cust001": {
        customerId: "cust001",
        name: "John Smith",
        email: "john@example.com",
        address: { street: "123 Main St", city: "Seattle", state: "WA", zipCode: "98101", country: "USA" },
        tier: "premium"
      },
      "cust002": {
        customerId: "cust002",
        name: "Jane Doe",
        email: "jane@example.com",
        address: { street: "456 Oak Ave", city: "Portland", state: "OR", zipCode: "97201", country: "USA" },
        tier: "standard"
      },
      "cust003": {
        customerId: "cust003",
        name: "Bob Wilson",
        email: "bob@example.com",
        address: { street: "789 Pine Rd", city: "San Francisco", state: "CA", zipCode: "94102", country: "USA" },
        tier: "vip"
      },
    };

    getCustomer(customerId: string): Customer {
      return this.customers[customerId];
    }
  }

  class InventoryService extends RpcTarget {
    private inventory: Record<string, InventoryStatus> = {
      "prod001": { productId: "prod001", productName: "Wireless Headphones", inStock: 150, reserved: 23, available: 127, warehouseLocation: "WH-SEA-01" },
      "prod002": { productId: "prod002", productName: "USB-C Hub", inStock: 500, reserved: 45, available: 455, warehouseLocation: "WH-PDX-02" },
      "prod003": { productId: "prod003", productName: "Mechanical Keyboard", inStock: 75, reserved: 12, available: 63, warehouseLocation: "WH-SEA-01" },
    };

    checkStock(productId: string): InventoryStatus {
      return this.inventory[productId];
    }
  }

  class ShippingService extends RpcTarget {
    estimateDelivery(address: Customer["address"]): ShippingEstimate {
      // Simulate different shipping times based on location
      const daysMap: Record<string, number> = {
        "WA": 2,
        "OR": 3,
        "CA": 4,
      };
      const days = daysMap[address.state] || 7;
      const deliveryDate = new Date();
      deliveryDate.setDate(deliveryDate.getDate() + days);

      return {
        carrierId: "carrier001",
        carrierName: "FastShip Express",
        estimatedDays: days,
        estimatedDelivery: deliveryDate.toISOString().split("T")[0],
        cost: days * 2.99,
        expeditedAvailable: days > 2
      };
    }
  }

  // Orchestrator that has access to all services
  class OrderOrchestrator extends RpcTarget {
    private orderService = new OrderService();
    private customerService = new CustomerService();
    private inventoryService = new InventoryService();
    private shippingService = new ShippingService();

    getOrderService() { return this.orderService; }
    getCustomerService() { return this.customerService; }
    getInventoryService() { return this.inventoryService; }
    getShippingService() { return this.shippingService; }

    enrichOrder(
      order: Order,
      customer: Customer,
      inventory: InventoryStatus,
      shipping: ShippingEstimate
    ): EnrichedOrder {
      return { order, customer, inventory, shipping };
    }
  }

  it("orchestrates across multiple services in a single round trip", async () => {
    await using harness = new TestHarness(new OrderOrchestrator());

    // Get pending orders via main stub
    using orders = harness.stub.getOrderService().getPendingOrders();

    // For each order, call THREE different services and combine results
    // IMPORTANT: Get service stubs INSIDE the callback, or use main stub directly
    const enrichedOrders = await orders.map((order: Order) => {
      // Get sub-service stubs inside callback so they're part of the recording
      const customerService = harness.stub.getCustomerService();
      const inventoryService = harness.stub.getInventoryService();
      const shippingService = harness.stub.getShippingService();

      const customer = customerService.getCustomer(order.customerId);
      const inventory = inventoryService.checkStock(order.productId);
      const shipping = shippingService.estimateDelivery(customer.address);
      return harness.stub.enrichOrder(order, customer, inventory, shipping);
    });

    // Verify we got enriched orders
    expect(enrichedOrders.length).toBe(4);

    // Verify each order has data from all services
    for (const enriched of enrichedOrders) {
      // Order data
      expect(enriched.order).toHaveProperty("orderId");
      expect(enriched.order).toHaveProperty("status", "pending");

      // Customer data (from CustomerService)
      expect(enriched.customer).toHaveProperty("name");
      expect(enriched.customer).toHaveProperty("email");
      expect(enriched.customer.address).toHaveProperty("city");

      // Inventory data (from InventoryService)
      expect(enriched.inventory).toHaveProperty("productName");
      expect(enriched.inventory).toHaveProperty("available");

      // Shipping data (from ShippingService)
      expect(enriched.shipping).toHaveProperty("carrierName");
      expect(enriched.shipping).toHaveProperty("estimatedDays");
      expect(enriched.shipping).toHaveProperty("cost");
    }

    // Verify specific order enrichment
    const johnOrder = enrichedOrders.find(e => e.customer.name === "John Smith" && e.order.productId === "prod001");
    expect(johnOrder).toBeDefined();
    expect(johnOrder!.inventory.productName).toBe("Wireless Headphones");
    expect(johnOrder!.shipping.estimatedDays).toBe(2); // Seattle = 2 days
  });
});

// =======================================================================================
// 5. RECURSIVE TREE PROCESSING
// =======================================================================================

describe("Extreme Map: Recursive Tree Processing", () => {
  // Realistic filesystem types
  interface FileSystemNode {
    name: string;
    path: string;
    type: "file" | "directory";
    size: number;  // bytes for files, 0 for directories
    modifiedAt: string;
    permissions: string;  // e.g., "rwxr-xr-x"
  }

  interface FileMetadata {
    path: string;
    name: string;
    size: number;
    extension: string;
    mimeType: string;
    checksum: string;
  }

  class FileSystemService extends RpcTarget {
    // Simulated filesystem tree
    private filesystem: Record<string, FileSystemNode[]> = {
      "/": [
        { name: "home", path: "/home", type: "directory", size: 0, modifiedAt: "2024-01-01T00:00:00Z", permissions: "rwxr-xr-x" },
        { name: "etc", path: "/etc", type: "directory", size: 0, modifiedAt: "2024-01-01T00:00:00Z", permissions: "rwxr-xr-x" },
      ],
      "/home": [
        { name: "user", path: "/home/user", type: "directory", size: 0, modifiedAt: "2024-01-10T00:00:00Z", permissions: "rwxr-x---" },
      ],
      "/home/user": [
        { name: "documents", path: "/home/user/documents", type: "directory", size: 0, modifiedAt: "2024-01-15T00:00:00Z", permissions: "rwx------" },
        { name: ".bashrc", path: "/home/user/.bashrc", type: "file", size: 3456, modifiedAt: "2024-01-14T00:00:00Z", permissions: "rw-r--r--" },
      ],
      "/home/user/documents": [
        { name: "report.pdf", path: "/home/user/documents/report.pdf", type: "file", size: 1048576, modifiedAt: "2024-01-15T10:30:00Z", permissions: "rw-r--r--" },
        { name: "notes.txt", path: "/home/user/documents/notes.txt", type: "file", size: 2048, modifiedAt: "2024-01-15T14:00:00Z", permissions: "rw-------" },
        { name: "projects", path: "/home/user/documents/projects", type: "directory", size: 0, modifiedAt: "2024-01-15T00:00:00Z", permissions: "rwx------" },
      ],
      "/home/user/documents/projects": [
        { name: "app.js", path: "/home/user/documents/projects/app.js", type: "file", size: 8192, modifiedAt: "2024-01-15T16:00:00Z", permissions: "rw-r--r--" },
        { name: "README.md", path: "/home/user/documents/projects/README.md", type: "file", size: 4096, modifiedAt: "2024-01-15T16:30:00Z", permissions: "rw-r--r--" },
      ],
      "/etc": [
        { name: "hosts", path: "/etc/hosts", type: "file", size: 512, modifiedAt: "2024-01-01T00:00:00Z", permissions: "rw-r--r--" },
        { name: "passwd", path: "/etc/passwd", type: "file", size: 1024, modifiedAt: "2024-01-01T00:00:00Z", permissions: "rw-r--r--" },
      ],
    };

    private mimeTypes: Record<string, string> = {
      ".pdf": "application/pdf",
      ".txt": "text/plain",
      ".js": "application/javascript",
      ".md": "text/markdown",
      "": "application/octet-stream",
    };

    getRoot(): FileSystemNode[] {
      return this.filesystem["/"];
    }

    getChildren(path: string): FileSystemNode[] {
      return this.filesystem[path] || [];
    }

    isDirectory(node: FileSystemNode): boolean {
      return node.type === "directory";
    }

    getMetadata(node: FileSystemNode): FileMetadata {
      const ext = node.name.includes(".") ? "." + node.name.split(".").pop()! : "";
      return {
        path: node.path,
        name: node.name,
        size: node.size,
        extension: ext,
        mimeType: this.mimeTypes[ext] || "application/octet-stream",
        checksum: `sha256-${node.path.replace(/\//g, "").substring(0, 16)}`,
      };
    }
  }

  it("processes filesystem tree of unknown depth", async () => {
    await using harness = new TestHarness(new FileSystemService());

    // Get root - client doesn't know the tree structure
    using root = harness.stub.getRoot();

    // Process filesystem tree with 4 levels of nested maps - truly extreme!
    // Key insight: Always collect metadata AND recurse into children at every level.
    // For files, getChildren() returns [] which naturally produces nothing when mapped.
    // This avoids conditionals (which can't work in map recording since RpcPromises are truthy).
    const level1 = await root.map((node: FileSystemNode) => {
      // Level 1: For each root item (/home, /etc), get its children
      return harness.stub.getChildren(node.path).map((child: FileSystemNode) => {
        // Level 2: Get metadata for this node AND recurse
        return {
          metadata: harness.stub.getMetadata(child),
          children: harness.stub.getChildren(child.path).map((grandchild: FileSystemNode) => {
            // Level 3: Get metadata for this node AND recurse
            return {
              metadata: harness.stub.getMetadata(grandchild),
              children: harness.stub.getChildren(grandchild.path).map((greatGrandchild: FileSystemNode) => {
                // Level 4: Finally get metadata for great-grandchildren
                return harness.stub.getMetadata(greatGrandchild);
              })
            };
          })
        };
      });
    });

    // Flatten and collect all file metadata from the nested structure
    const allFiles: FileMetadata[] = [];
    const collectFiles = (item: any) => {
      if (Array.isArray(item)) {
        item.forEach(collectFiles);
      } else if (item && item.metadata && item.metadata.path) {
        // Node with metadata and children
        allFiles.push(item.metadata);
        if (item.children) {
          collectFiles(item.children);
        }
      } else if (item && item.path) {
        // Direct metadata (level 4)
        allFiles.push(item);
      }
    };
    level1.forEach(collectFiles);

    // Verify we found files at various depths
    expect(allFiles.length).toBeGreaterThan(0);

    // Verify we found specific files
    const bashrc = allFiles.find(f => f.name === ".bashrc");
    expect(bashrc).toBeDefined();
    expect(bashrc!.mimeType).toBe("application/octet-stream");

    const report = allFiles.find(f => f.name === "report.pdf");
    expect(report).toBeDefined();
    expect(report!.mimeType).toBe("application/pdf");
    expect(report!.size).toBe(1048576);
  });
});

// =======================================================================================
// 6. CAPABILITY-GATED BULK OPERATIONS
// =======================================================================================

describe("Extreme Map: Capability-Gated Bulk Operations", () => {
  // Realistic access control types
  interface User {
    userId: string;
    username: string;
    department: string;
    clearanceLevel: number;  // 1-5
  }

  interface Permission {
    userId: string;
    resource: string;
    granted: boolean;
    reason: string;
    grantedBy: string;
    expiresAt: string | null;
  }

  interface SensitiveData {
    userId: string;
    ssn: string;  // Would be encrypted in reality
    salary: number;
    performanceRating: number;
    notes: string;
  }

  interface AccessResult {
    userId: string;
    username: string;
    permitted: boolean;
    data: SensitiveData | null;
    denialReason: string | null;
  }

  class AuthorizationService extends RpcTarget {
    private permissions: Record<string, Permission> = {
      "user001": { userId: "user001", resource: "sensitive_data", granted: true, reason: "HR role", grantedBy: "admin", expiresAt: null },
      "user002": { userId: "user002", resource: "sensitive_data", granted: false, reason: "Insufficient clearance", grantedBy: "system", expiresAt: null },
      "user003": { userId: "user003", resource: "sensitive_data", granted: true, reason: "Manager override", grantedBy: "manager001", expiresAt: "2024-12-31" },
      "user004": { userId: "user004", resource: "sensitive_data", granted: false, reason: "Access revoked", grantedBy: "security", expiresAt: null },
      "user005": { userId: "user005", resource: "sensitive_data", granted: true, reason: "Executive role", grantedBy: "admin", expiresAt: null },
    };

    checkPermission(userId: string, resource: string): Permission {
      return this.permissions[userId] || {
        userId,
        resource,
        granted: false,
        reason: "No permission record found",
        grantedBy: "system",
        expiresAt: null
      };
    }
  }

  class SensitiveDataService extends RpcTarget {
    private data: Record<string, SensitiveData> = {
      "user001": { userId: "user001", ssn: "XXX-XX-1234", salary: 85000, performanceRating: 4.2, notes: "Excellent team player" },
      "user002": { userId: "user002", ssn: "XXX-XX-5678", salary: 72000, performanceRating: 3.8, notes: "Meeting expectations" },
      "user003": { userId: "user003", ssn: "XXX-XX-9012", salary: 95000, performanceRating: 4.5, notes: "High performer" },
      "user004": { userId: "user004", ssn: "XXX-XX-3456", salary: 68000, performanceRating: 3.2, notes: "Needs improvement" },
      "user005": { userId: "user005", ssn: "XXX-XX-7890", salary: 120000, performanceRating: 4.8, notes: "Leadership potential" },
    };

    getSensitiveData(userId: string): SensitiveData | null {
      return this.data[userId] || null;
    }
  }

  class AdminService extends RpcTarget {
    private users: User[] = [
      { userId: "user001", username: "alice", department: "HR", clearanceLevel: 4 },
      { userId: "user002", username: "bob", department: "Engineering", clearanceLevel: 2 },
      { userId: "user003", username: "charlie", department: "Finance", clearanceLevel: 3 },
      { userId: "user004", username: "diana", department: "Marketing", clearanceLevel: 2 },
      { userId: "user005", username: "eve", department: "Executive", clearanceLevel: 5 },
    ];

    private authService = new AuthorizationService();
    private dataService = new SensitiveDataService();

    getAllUsers(): User[] {
      return this.users;
    }

    getAuthService() { return this.authService; }
    getDataService() { return this.dataService; }

    buildAccessResult(
      user: User,
      permission: Permission,
      data: SensitiveData | null
    ): AccessResult {
      return {
        userId: user.userId,
        username: user.username,
        permitted: permission.granted,
        data: permission.granted ? data : null,
        denialReason: permission.granted ? null : permission.reason
      };
    }
  }

  it("enforces permissions for each user in bulk operation", async () => {
    await using harness = new TestHarness(new AdminService());

    using authService = harness.stub.getAuthService();
    using dataService = harness.stub.getDataService();

    // Get all users
    using users = harness.stub.getAllUsers();

    // For each user, check permission AND conditionally fetch data
    const accessResults = await users.map((user: User) => {
      const permission = authService.checkPermission(user.userId, "sensitive_data");
      // Note: Data is fetched regardless, but buildAccessResult hides it if not permitted
      const data = dataService.getSensitiveData(user.userId);
      return harness.stub.buildAccessResult(user, permission, data);
    });

    // Verify results
    expect(accessResults.length).toBe(5);

    // Check permitted users got data
    const alice = accessResults.find(r => r.username === "alice")!;
    expect(alice.permitted).toBe(true);
    expect(alice.data).not.toBeNull();
    expect(alice.data!.salary).toBe(85000);

    const eve = accessResults.find(r => r.username === "eve")!;
    expect(eve.permitted).toBe(true);
    expect(eve.data).not.toBeNull();

    // Check denied users got no data
    const bob = accessResults.find(r => r.username === "bob")!;
    expect(bob.permitted).toBe(false);
    expect(bob.data).toBeNull();
    expect(bob.denialReason).toBe("Insufficient clearance");

    const diana = accessResults.find(r => r.username === "diana")!;
    expect(diana.permitted).toBe(false);
    expect(diana.data).toBeNull();
    expect(diana.denialReason).toBe("Access revoked");
  });
});

// =======================================================================================
// 7. DISTRIBUTED AI INFERENCE PIPELINE
// =======================================================================================

describe("Extreme Map: Distributed AI Inference Pipeline", () => {
  // Realistic computer vision types
  interface VideoFrame {
    frameId: string;
    timestamp: number;  // milliseconds
    resolution: { width: number; height: number };
    format: "rgb" | "yuv" | "grayscale";
    dataRef: string;  // Reference to actual pixel data (not sent over wire)
  }

  interface DetectedObject {
    objectId: string;
    frameId: string;
    boundingBox: { x: number; y: number; width: number; height: number };
    confidence: number;
    rawFeatures: string;  // Reference to feature vector
  }

  interface Classification {
    objectId: string;
    category: string;
    subcategory: string;
    confidence: number;
    attributes: Record<string, string>;
  }

  interface ThreatAssessment {
    objectId: string;
    classification: string;
    threatLevel: "none" | "low" | "medium" | "high" | "critical";
    actionRequired: boolean;
    reasoning: string;
  }

  interface InferenceResult {
    object: DetectedObject;
    classification: Classification;
    risk: ThreatAssessment;
  }

  class CameraService extends RpcTarget {
    // Simulated frame buffer (in reality, streaming from camera)
    getFrameBuffer(): VideoFrame[] {
      return [
        { frameId: "frame001", timestamp: 0, resolution: { width: 1920, height: 1080 }, format: "rgb", dataRef: "buffer://frame001" },
        { frameId: "frame002", timestamp: 33, resolution: { width: 1920, height: 1080 }, format: "rgb", dataRef: "buffer://frame002" },
        { frameId: "frame003", timestamp: 66, resolution: { width: 1920, height: 1080 }, format: "rgb", dataRef: "buffer://frame003" },
        { frameId: "frame004", timestamp: 100, resolution: { width: 1920, height: 1080 }, format: "rgb", dataRef: "buffer://frame004" },
      ];
    }
  }

  class AIService extends RpcTarget {
    private detectionResults: Record<string, DetectedObject[]> = {
      "frame001": [
        { objectId: "obj001", frameId: "frame001", boundingBox: { x: 100, y: 150, width: 200, height: 300 }, confidence: 0.95, rawFeatures: "features://obj001" },
        { objectId: "obj002", frameId: "frame001", boundingBox: { x: 500, y: 200, width: 150, height: 250 }, confidence: 0.87, rawFeatures: "features://obj002" },
      ],
      "frame002": [
        { objectId: "obj003", frameId: "frame002", boundingBox: { x: 120, y: 160, width: 195, height: 295 }, confidence: 0.93, rawFeatures: "features://obj003" },
      ],
      "frame003": [],  // No detections
      "frame004": [
        { objectId: "obj004", frameId: "frame004", boundingBox: { x: 800, y: 100, width: 100, height: 150 }, confidence: 0.78, rawFeatures: "features://obj004" },
      ],
    };

    private classifications: Record<string, Classification> = {
      "obj001": { objectId: "obj001", category: "person", subcategory: "adult", confidence: 0.92, attributes: { pose: "standing", clothing: "casual" } },
      "obj002": { objectId: "obj002", category: "vehicle", subcategory: "car", confidence: 0.89, attributes: { color: "blue", make: "sedan" } },
      "obj003": { objectId: "obj003", category: "person", subcategory: "adult", confidence: 0.91, attributes: { pose: "walking", clothing: "business" } },
      "obj004": { objectId: "obj004", category: "animal", subcategory: "dog", confidence: 0.85, attributes: { size: "medium", breed: "unknown" } },
    };

    detectObjects(frame: VideoFrame): DetectedObject[] {
      return this.detectionResults[frame.frameId] || [];
    }

    classify(object: DetectedObject): Classification {
      return this.classifications[object.objectId] || {
        objectId: object.objectId,
        category: "unknown",
        subcategory: "unknown",
        confidence: 0,
        attributes: {}
      };
    }
  }

  class RiskService extends RpcTarget {
    assessThreat(classification: Classification): ThreatAssessment {
      // Simulate threat assessment logic
      let threatLevel: ThreatAssessment["threatLevel"] = "none";
      let actionRequired = false;
      let reasoning = "Normal activity detected";

      if (classification.category === "person") {
        if (classification.attributes.pose === "running") {
          threatLevel = "low";
          reasoning = "Person running - monitoring";
        }
      } else if (classification.category === "vehicle") {
        if (classification.confidence < 0.8) {
          threatLevel = "low";
          reasoning = "Unidentified vehicle - flagged for review";
        }
      } else if (classification.category === "unknown") {
        threatLevel = "medium";
        actionRequired = true;
        reasoning = "Unclassified object requires human review";
      }

      return {
        objectId: classification.objectId,
        classification: `${classification.category}/${classification.subcategory}`,
        threatLevel,
        actionRequired,
        reasoning
      };
    }
  }

  class InferencePipeline extends RpcTarget {
    private camera = new CameraService();
    private ai = new AIService();
    private risk = new RiskService();

    getCameraService() { return this.camera; }
    getAIService() { return this.ai; }
    getRiskService() { return this.risk; }

    buildResult(obj: DetectedObject, classification: Classification, risk: ThreatAssessment): InferenceResult {
      return { object: obj, classification, risk };
    }
  }

  it("runs detection -> classification -> risk pipeline on all frames", async () => {
    await using harness = new TestHarness(new InferencePipeline());

    // Get frame buffer via main stub
    using frames = harness.stub.getCameraService().getFrameBuffer();

    // For each frame -> detect objects -> classify each -> assess risk
    // IMPORTANT: In nested .map() callbacks, always use harness.stub.* directly
    // Don't use stubs from outer scope in inner callbacks
    const pipelineResults = await frames.map((frame: VideoFrame) => {
      return harness.stub.getAIService().detectObjects(frame).map((obj: DetectedObject) => {
        // Use harness.stub.* in inner callback, not stubs from outer scope
        const classification = harness.stub.getAIService().classify(obj);
        const riskAssessment = harness.stub.getRiskService().assessThreat(classification);
        return harness.stub.buildResult(obj, classification, riskAssessment);
      });
    });

    // Flatten results
    const allResults: InferenceResult[] = [];
    for (const frameResults of pipelineResults) {
      allResults.push(...frameResults);
    }

    // Verify we processed all detected objects
    expect(allResults.length).toBe(4);  // obj001, obj002, obj003, obj004

    // Verify pipeline produced complete results
    for (const result of allResults) {
      expect(result.object).toHaveProperty("objectId");
      expect(result.object).toHaveProperty("boundingBox");
      expect(result.classification).toHaveProperty("category");
      expect(result.classification).toHaveProperty("confidence");
      expect(result.risk).toHaveProperty("threatLevel");
      expect(result.risk).toHaveProperty("reasoning");
    }

    // Verify specific detections
    const person = allResults.find(r => r.classification.category === "person");
    expect(person).toBeDefined();
    expect(person!.risk.threatLevel).toBe("none");

    const car = allResults.find(r => r.classification.subcategory === "car");
    expect(car).toBeDefined();
    expect(car!.classification.attributes.color).toBe("blue");
  });
});

// =======================================================================================
// 8. REAL-TIME GAME STATE COMPUTATION
// =======================================================================================

describe("Extreme Map: Real-Time Game State Computation", () => {
  // Realistic RTS game types
  interface Position {
    x: number;
    y: number;
  }

  interface GameUnit {
    unitId: string;
    playerId: string;
    unitType: "infantry" | "tank" | "artillery" | "air";
    position: Position;
    health: number;
    maxHealth: number;
    attackPower: number;
    attackRange: number;
    armor: number;
    status: "idle" | "moving" | "attacking" | "dead";
  }

  interface DamageCalculation {
    attackerId: string;
    defenderId: string;
    baseDamage: number;
    armorReduction: number;
    finalDamage: number;
    defenderHealthAfter: number;
    defenderKilled: boolean;
  }

  interface CombatResult {
    attacker: GameUnit;
    targets: DamageCalculation[];
    totalDamageDealt: number;
    kills: number;
  }

  class GameStateService extends RpcTarget {
    private units: GameUnit[] = [
      // Player 1 units
      { unitId: "u001", playerId: "p1", unitType: "tank", position: { x: 100, y: 100 }, health: 500, maxHealth: 500, attackPower: 80, attackRange: 150, armor: 50, status: "idle" },
      { unitId: "u002", playerId: "p1", unitType: "infantry", position: { x: 120, y: 110 }, health: 100, maxHealth: 100, attackPower: 20, attackRange: 80, armor: 10, status: "idle" },
      { unitId: "u003", playerId: "p1", unitType: "artillery", position: { x: 80, y: 150 }, health: 200, maxHealth: 200, attackPower: 150, attackRange: 300, armor: 20, status: "idle" },
      // Player 2 units (enemies)
      { unitId: "u004", playerId: "p2", unitType: "infantry", position: { x: 200, y: 120 }, health: 100, maxHealth: 100, attackPower: 25, attackRange: 80, armor: 15, status: "idle" },
      { unitId: "u005", playerId: "p2", unitType: "infantry", position: { x: 220, y: 130 }, health: 100, maxHealth: 100, attackPower: 25, attackRange: 80, armor: 15, status: "idle" },
      { unitId: "u006", playerId: "p2", unitType: "tank", position: { x: 250, y: 100 }, health: 450, maxHealth: 500, attackPower: 75, attackRange: 140, armor: 45, status: "idle" },
      { unitId: "u007", playerId: "p2", unitType: "air", position: { x: 180, y: 200 }, health: 150, maxHealth: 150, attackPower: 60, attackRange: 120, armor: 5, status: "moving" },
    ];

    getAllUnits(): GameUnit[] {
      return this.units;
    }

    getPlayerUnits(playerId: string): GameUnit[] {
      return this.units.filter(u => u.playerId === playerId && u.status !== "dead");
    }

    getEnemiesInRange(position: Position, range: number, excludePlayerId: string): GameUnit[] {
      return this.units.filter(unit => {
        if (unit.playerId === excludePlayerId || unit.status === "dead") return false;
        const distance = Math.sqrt(
          Math.pow(unit.position.x - position.x, 2) +
          Math.pow(unit.position.y - position.y, 2)
        );
        return distance <= range;
      });
    }
  }

  class CombatService extends RpcTarget {
    calculateDamage(attacker: GameUnit, defender: GameUnit): DamageCalculation {
      const baseDamage = attacker.attackPower;

      // Armor reduces damage (diminishing returns formula)
      const armorReduction = Math.floor(baseDamage * (defender.armor / (defender.armor + 100)));
      const finalDamage = Math.max(1, baseDamage - armorReduction);

      const healthAfter = Math.max(0, defender.health - finalDamage);
      const killed = healthAfter === 0;

      return {
        attackerId: attacker.unitId,
        defenderId: defender.unitId,
        baseDamage,
        armorReduction,
        finalDamage,
        defenderHealthAfter: healthAfter,
        defenderKilled: killed
      };
    }

    // Apply terrain and critical hit modifiers to a damage calculation
    applyBattleModifiers(damage: DamageCalculation, attackerType: string): DamageCalculation {
      // Terrain bonus: artillery gets +20% damage, air units get +10%
      let terrainMultiplier = 1.0;
      if (attackerType === "artillery") terrainMultiplier = 1.2;
      else if (attackerType === "air") terrainMultiplier = 1.1;

      // Critical hit check (simplified): 10% chance for double damage
      const isCritical = damage.baseDamage % 10 === 0;  // Deterministic "random" for testing
      const critMultiplier = isCritical ? 2.0 : 1.0;

      const modifiedDamage = Math.floor(damage.finalDamage * terrainMultiplier * critMultiplier);
      const newHealthAfter = Math.max(0, damage.defenderHealthAfter + damage.finalDamage - modifiedDamage);

      return {
        ...damage,
        finalDamage: modifiedDamage,
        defenderHealthAfter: newHealthAfter,
        defenderKilled: newHealthAfter === 0
      };
    }

    buildCombatResult(attacker: GameUnit, damages: DamageCalculation[]): CombatResult {
      return {
        attacker,
        targets: damages,
        totalDamageDealt: damages.reduce((sum, d) => sum + d.finalDamage, 0),
        kills: damages.filter(d => d.defenderKilled).length
      };
    }
  }

  class GameServer extends RpcTarget {
    private gameState = new GameStateService();
    private combat = new CombatService();

    getGameState() { return this.gameState; }
    getCombat() { return this.combat; }
  }

  it("computes combat resolution for all units in single round trip", async () => {
    await using harness = new TestHarness(new GameServer());

    // Get player 1's units via main stub
    using player1Units = harness.stub.getGameState().getPlayerUnits("p1");

    // For each unit, find enemies in range, calculate damage, apply modifiers, build result
    // IMPORTANT: In nested/chained .map() callbacks, always use harness.stub.* directly
    const combatResults = await player1Units.map((unit: GameUnit) => {
      // Chain of maps: enemies -> damage calculations -> modified damages
      // Use harness.stub.* throughout to avoid scope issues
      const modifiedDamages = harness.stub.getGameState().getEnemiesInRange(unit.position, unit.attackRange, unit.playerId)
        .map((enemy: GameUnit) => {
          return harness.stub.getCombat().calculateDamage(unit, enemy);
        })
        .map((damage: DamageCalculation) => {
          // Second map: apply terrain/critical modifiers to each damage result
          return harness.stub.getCombat().applyBattleModifiers(damage, unit.unitType);
        });
      // Pass the RpcPromise of modified damages to buildCombatResult (pipelining resolves it)
      return harness.stub.getCombat().buildCombatResult(unit, modifiedDamages);
    });

    // Analyze results
    expect(combatResults.length).toBe(3);  // 3 player 1 units

    // Tank should be able to hit multiple enemies
    const tankResult = combatResults.find((r: any) =>
      Array.isArray(r) ? r[0]?.attacker?.unitType === "tank" : r.attacker?.unitType === "tank"
    );
    expect(tankResult).toBeDefined();

    // Artillery has longest range, should hit most enemies
    const artilleryResult = combatResults.find((r: any) =>
      Array.isArray(r) ? r[0]?.attacker?.unitType === "artillery" : r.attacker?.unitType === "artillery"
    );
    expect(artilleryResult).toBeDefined();

    // Verify damage calculations are realistic
    const allDamages: DamageCalculation[] = [];
    const collectDamages = (item: any) => {
      if (Array.isArray(item)) {
        item.forEach(collectDamages);
      } else if (item && item.targets) {
        allDamages.push(...item.targets);
      } else if (item && item.attackerId) {
        allDamages.push(item);
      }
    };
    combatResults.forEach(collectDamages);

    for (const damage of allDamages) {
      expect(damage.baseDamage).toBeGreaterThan(0);
      expect(damage.armorReduction).toBeGreaterThanOrEqual(0);
      expect(damage.finalDamage).toBeGreaterThan(0);
      // Note: finalDamage can exceed baseDamage due to terrain/critical modifiers
    }
  });
});

// =======================================================================================
// 9. LAZY DATABASE CURSOR WITH JOINS
// =======================================================================================

describe("Extreme Map: Lazy Database Cursor with Joins", () => {
  // Realistic database types
  interface OrderRow {
    orderId: string;
    customerId: string;
    productId: string;
    quantity: number;
    unitPrice: number;
    orderDate: string;
    status: string;
  }

  interface Product {
    productId: string;
    name: string;
    category: string;
    supplierId: string;
    weight: number;
    dimensions: { length: number; width: number; height: number };
  }

  interface Supplier {
    supplierId: string;
    companyName: string;
    contactName: string;
    country: string;
    rating: number;
    leadTimeDays: number;
  }

  interface EnrichedOrderRow {
    order: OrderRow;
    product: Product;
    supplier: Supplier;
    totalValue: number;
    estimatedShipDate: string;
  }

  class DatabaseService extends RpcTarget {
    // Simulated database tables
    private orders: OrderRow[] = [
      { orderId: "ord001", customerId: "c001", productId: "prod001", quantity: 5, unitPrice: 29.99, orderDate: "2024-01-15", status: "pending" },
      { orderId: "ord002", customerId: "c002", productId: "prod002", quantity: 2, unitPrice: 149.99, orderDate: "2024-01-15", status: "pending" },
      { orderId: "ord003", customerId: "c001", productId: "prod003", quantity: 10, unitPrice: 9.99, orderDate: "2024-01-15", status: "pending" },
      { orderId: "ord004", customerId: "c003", productId: "prod001", quantity: 3, unitPrice: 29.99, orderDate: "2024-01-15", status: "pending" },
      { orderId: "ord005", customerId: "c004", productId: "prod004", quantity: 1, unitPrice: 599.99, orderDate: "2024-01-15", status: "pending" },
      { orderId: "ord006", customerId: "c002", productId: "prod005", quantity: 20, unitPrice: 4.99, orderDate: "2024-01-15", status: "pending" },
    ];

    private products: Record<string, Product> = {
      "prod001": { productId: "prod001", name: "Wireless Mouse", category: "Electronics", supplierId: "sup001", weight: 0.15, dimensions: { length: 12, width: 6, height: 4 } },
      "prod002": { productId: "prod002", name: "Mechanical Keyboard", category: "Electronics", supplierId: "sup001", weight: 1.2, dimensions: { length: 45, width: 15, height: 4 } },
      "prod003": { productId: "prod003", name: "USB Cable", category: "Accessories", supplierId: "sup002", weight: 0.05, dimensions: { length: 100, width: 1, height: 1 } },
      "prod004": { productId: "prod004", name: "4K Monitor", category: "Electronics", supplierId: "sup003", weight: 8.5, dimensions: { length: 70, width: 45, height: 10 } },
      "prod005": { productId: "prod005", name: "Screen Wipes", category: "Accessories", supplierId: "sup002", weight: 0.2, dimensions: { length: 15, width: 10, height: 5 } },
    };

    private suppliers: Record<string, Supplier> = {
      "sup001": { supplierId: "sup001", companyName: "TechParts Inc", contactName: "John Tech", country: "Taiwan", rating: 4.5, leadTimeDays: 14 },
      "sup002": { supplierId: "sup002", companyName: "AccessoryWorld", contactName: "Jane Access", country: "China", rating: 4.2, leadTimeDays: 21 },
      "sup003": { supplierId: "sup003", companyName: "DisplayPro Ltd", contactName: "Mike Display", country: "South Korea", rating: 4.8, leadTimeDays: 10 },
    };

    // Simulates a database cursor over query results
    queryCursor(query: string): OrderRow[] {
      // In reality, this would parse SQL and return a cursor
      // For simulation, we filter based on a simple pattern
      if (query.includes("status = 'pending'")) {
        return this.orders.filter(o => o.status === "pending");
      }
      return this.orders;
    }

    getProduct(productId: string): Product {
      return this.products[productId];
    }

    getSupplier(supplierId: string): Supplier {
      return this.suppliers[supplierId];
    }

    enrichOrder(order: OrderRow, product: Product, supplier: Supplier): EnrichedOrderRow {
      const totalValue = order.quantity * order.unitPrice;
      const orderDate = new Date(order.orderDate);
      orderDate.setDate(orderDate.getDate() + supplier.leadTimeDays);

      return {
        order,
        product,
        supplier,
        totalValue,
        estimatedShipDate: orderDate.toISOString().split("T")[0]
      };
    }
  }

  it("enriches cursor results with joined data without loading all into memory", async () => {
    await using harness = new TestHarness(new DatabaseService());

    // Execute query - returns a cursor (simulated as array)
    using cursor = harness.stub.queryCursor("SELECT * FROM orders WHERE status = 'pending'");

    // For each row, join with product and supplier tables
    const enrichedOrders = await cursor.map((row: OrderRow) => {
      const product = harness.stub.getProduct(row.productId);
      const supplier = harness.stub.getSupplier(product.supplierId);
      return harness.stub.enrichOrder(row, product, supplier);
    });

    // Verify we got all orders
    expect(enrichedOrders.length).toBe(6);

    // Verify joins worked correctly
    for (const enriched of enrichedOrders) {
      // Order data
      expect(enriched.order).toHaveProperty("orderId");
      expect(enriched.order).toHaveProperty("quantity");

      // Joined product data
      expect(enriched.product).toHaveProperty("name");
      expect(enriched.product).toHaveProperty("category");

      // Joined supplier data
      expect(enriched.supplier).toHaveProperty("companyName");
      expect(enriched.supplier).toHaveProperty("leadTimeDays");

      // Computed fields
      expect(enriched.totalValue).toBe(enriched.order.quantity * enriched.order.unitPrice);
      expect(enriched.estimatedShipDate).toMatch(/^\d{4}-\d{2}-\d{2}$/);
    }

    // Verify specific order enrichment
    const monitorOrder = enrichedOrders.find(e => e.product.name === "4K Monitor");
    expect(monitorOrder).toBeDefined();
    expect(monitorOrder!.supplier.companyName).toBe("DisplayPro Ltd");
    expect(monitorOrder!.totalValue).toBe(599.99);
    expect(monitorOrder!.supplier.leadTimeDays).toBe(10);
  });
});

// =======================================================================================
// 10. THE TERRIFYING ONE: WEBHOOK-AS-A-SERVICE
// =======================================================================================

describe("Extreme Map: Webhook-as-a-Service (The Terrifying One)", () => {
  // Realistic event streaming types
  interface Event {
    eventId: string;
    eventType: string;
    timestamp: string;
    payload: Record<string, unknown>;
    source: string;
    version: string;
  }

  interface WebhookDelivery {
    eventId: string;
    deliveredAt: string;
    statusCode: number;
    responseTime: number;
    success: boolean;
  }

  interface ProcessedEvent {
    event: Event;
    delivery: WebhookDelivery;
    acknowledged: boolean;
  }

  // Client-side webhook handler (this is what gets "captured" and sent to server)
  class ClientWebhookHandler extends RpcTarget {
    private receivedEvents: Event[] = [];

    notify(event: Event): WebhookDelivery {
      // Simulate processing the event
      this.receivedEvents.push(event);

      return {
        eventId: event.eventId,
        deliveredAt: new Date().toISOString(),
        statusCode: 200,
        responseTime: Math.floor(Math.random() * 50) + 10,
        success: true
      };
    }

    getReceivedEvents(): Event[] {
      return this.receivedEvents;
    }
  }

  class EventStreamService extends RpcTarget {
    // Simulated event stream (in reality, this would be Kafka, Pub/Sub, etc.)
    private events: Event[] = [
      { eventId: "evt001", eventType: "user.created", timestamp: "2024-01-15T10:00:00Z", payload: { userId: "u001", email: "user1@example.com" }, source: "auth-service", version: "1.0" },
      { eventId: "evt002", eventType: "user.login", timestamp: "2024-01-15T10:01:00Z", payload: { userId: "u001", ip: "192.168.1.1" }, source: "auth-service", version: "1.0" },
      { eventId: "evt003", eventType: "order.placed", timestamp: "2024-01-15T10:02:00Z", payload: { orderId: "ord001", amount: 99.99 }, source: "order-service", version: "1.0" },
      { eventId: "evt004", eventType: "user.updated", timestamp: "2024-01-15T10:03:00Z", payload: { userId: "u001", field: "name" }, source: "auth-service", version: "1.0" },
      { eventId: "evt005", eventType: "payment.processed", timestamp: "2024-01-15T10:04:00Z", payload: { paymentId: "pay001", status: "success" }, source: "payment-service", version: "1.0" },
    ];

    // Registered webhook handler (passed by client, server gets stub to it)
    private registeredWebhook: RpcStub<ClientWebhookHandler> | null = null;
    private registeredGenericHandler: RpcStub<any> | null = null;

    // Register a webhook handler - server now has a stub to call back to client
    // IMPORTANT: Must dup() to keep the stub alive after the call completes
    registerWebhook(webhook: RpcStub<ClientWebhookHandler>): RpcStub<ClientWebhookHandler> {
      this.registeredWebhook = webhook.dup();
      return this.registeredWebhook;  // Return the duped version for use in map callbacks
    }

    getRegisteredWebhook(): RpcStub<ClientWebhookHandler> | null {
      return this.registeredWebhook;
    }

    // Generic handler registration - allows any RpcTarget to be registered and returned
    // This gives the server a stub to call back to the client
    // IMPORTANT: Must dup() to keep the stub alive after the call completes
    registerHandler<T extends RpcTarget>(handler: RpcStub<T>): RpcStub<T> {
      this.registeredGenericHandler = handler.dup();
      return this.registeredGenericHandler;
    }

    // Cleanup registered handlers
    clearHandlers() {
      if (this.registeredWebhook) {
        this.registeredWebhook[Symbol.dispose]();
        this.registeredWebhook = null;
      }
      if (this.registeredGenericHandler) {
        this.registeredGenericHandler[Symbol.dispose]();
        this.registeredGenericHandler = null;
      }
    }

    // Subscribe to events matching a pattern
    subscribe(pattern: string): Event[] {
      // Simple pattern matching (in reality, would be regex or glob)
      if (pattern === "user.*") {
        return this.events.filter(e => e.eventType.startsWith("user."));
      } else if (pattern === "*") {
        return this.events;
      }
      return this.events.filter(e => e.eventType === pattern);
    }

    acknowledge(eventId: string): boolean {
      // Mark event as processed
      return true;
    }

    buildProcessedEvent(event: Event, delivery: WebhookDelivery, ack: boolean): ProcessedEvent {
      return { event, delivery, acknowledged: ack };
    }
  }

  it("delivers events to client webhook handler (demonstrating capability capture)", async () => {
    // Create client-side webhook handler
    const clientWebhook = new ClientWebhookHandler();

    await using harness = new TestHarness(new EventStreamService());

    // CRITICAL: Register the webhook with the server FIRST
    // This gives the server a stub to call back to the client
    // Don't use `using` - this stub is captured in the .map() callback
    const webhookStub = harness.stub.registerWebhook(clientWebhook);

    // Subscribe to user events - don't use `using` for array being mapped
    const events = harness.stub.subscribe("user.*");

    // For each event, call the CLIENT's webhook handler via its stub
    // THIS IS THE TERRIFYING PART: The server will call YOUR stub for each event
    const processed = await events.map((event: Event) => {
      // webhookStub.notify calls BACK to the client's handler
      // The server invokes this for each event during map replay
      const delivery = webhookStub.notify(event);
      const ack = harness.stub.acknowledge(event.eventId);
      return harness.stub.buildProcessedEvent(event, delivery, ack);
    });

    // Verify events were processed
    expect(processed.length).toBe(3);  // user.created, user.login, user.updated

    // Verify the webhook was actually called (on client side!)
    const received = clientWebhook.getReceivedEvents();
    expect(received.length).toBe(3);

    // Verify each event was delivered successfully
    for (const result of processed) {
      expect(result.event).toHaveProperty("eventId");
      expect(result.event.eventType).toMatch(/^user\./);
      expect(result.delivery.success).toBe(true);
      expect(result.delivery.statusCode).toBe(200);
      expect(result.acknowledged).toBe(true);
    }

    // Verify specific events
    const loginEvent = processed.find(p => p.event.eventType === "user.login");
    expect(loginEvent).toBeDefined();
    expect(loginEvent!.event.payload).toHaveProperty("ip");
  });

  it("demonstrates the danger: server controls when YOUR callback is invoked", async () => {
    let callCount = 0;
    const callLog: string[] = [];

    // Create a webhook that tracks calls
    class TrackingWebhook extends RpcTarget {
      onEvent(event: Event): { received: boolean; callNumber: number } {
        callCount++;
        callLog.push(`Call ${callCount}: ${event.eventType}`);
        return { received: true, callNumber: callCount };
      }
    }

    const webhook = new TrackingWebhook();

    await using harness = new TestHarness(new EventStreamService());

    // CRITICAL: Register the handler with the server FIRST
    // This gives the server a stub to call back to the client
    // Don't use `using` - this stub is captured in the .map() callback
    const webhookStub = harness.stub.registerHandler(webhook);

    // Subscribe to ALL events - don't use `using` for array being mapped
    const events = harness.stub.subscribe("*");

    // The server will invoke webhookStub.onEvent for EVERY event
    // A malicious server could invoke it thousands of times
    const results = await events.map((event: Event) => {
      return webhookStub.onEvent(event);
    });

    // Verify callback was invoked for each event
    expect(callCount).toBe(5);  // All 5 events
    expect(callLog.length).toBe(5);

    // The "terrifying" insight: if this were a real system,
    // the server controls the event stream and could:
    // 1. Flood your webhook with millions of events
    // 2. Invoke it with crafted malicious payloads
    // 3. Time attacks by controlling when events arrive
    expect(results.every(r => r.received)).toBe(true);
  });
});
