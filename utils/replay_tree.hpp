// Enhanced order-statistics version of the btree from the tlx library, which is
// under the Boost Software License 1.0

#pragma once
#ifndef REPLAY_TREE_HPP_INCLUDED
#define REPLAY_TREE_HPP_INCLUDED

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <functional>
#include <istream>
#include <limits>
#include <memory>
#include <ostream>
#include <utility>

// *** Debugging Macros

#ifdef REPLAY_TREE_DEBUG

//! Assertion only if REPLAY_TREE_DEBUG is defined. This is not used in verify().
#define REPLAY_TREE_ASSERT(x) \
    do {                      \
        assert(x);            \
    } while (0)

#else

//! Assertion only if REPLAY_TREE_DEBUG is defined. This is not used in verify().
#define REPLAY_TREE_ASSERT(x) \
    do {                      \
    } while (0)

#endif

/*!
 * Generates default traits for a tree used as a set or map. It estimates
 * leaf and inner node sizes by assuming a cache line multiple of 256 bytes.
 */
template <typename Key, typename Value>
struct btree_default_traits {
    //! If true, the tree will self verify its invariants after each insert() or
    //! erase(). The header must have been compiled with REPLAY_TREE_DEBUG
    //! defined.
    static constexpr bool self_verify = false;

    //! If true, the tree will print out debug information and a tree dump
    //! during insert() or erase() operation. The header must have been
    //! compiled with REPLAY_TREE_DEBUG defined and key_type must be std::ostream
    //! printable.
    static constexpr bool debug = true;

    //! Number of slots in each leaf of the tree. Estimated so that each node
    //! has a size of about 256 bytes.
    static constexpr int leaf_slots = 8 > (256 / (sizeof(Value))) ? 8 : (256 / (sizeof(Value)));

    //! Number of slots in each inner node of the tree. Estimated so that each
    //! node has a size of about 256 bytes.
    static const int inner_slots = 8 > (256 / (sizeof(Key) + sizeof(void*))) ? 8
                                                                             : (256 / (sizeof(Key) + sizeof(void*)));

    //! As of stx-btree-0.9, the code does linear search in find_lower() and
    //! find_upper() instead of binary_search, unless the node size is larger
    //! than this threshold. See notes at
    //! http://panthema.net/2013/0504-STX-B+Tree-Binary-vs-Linear-Search
    static const size_t binsearch_threshold = 256;
};

template <typename Node, bool IsConst>
class TreeIterator;

struct node {
    //! Level in the b-tree, if level == 0 -> leaf node
    unsigned short level;

    //! Number of key slotuse use, so the number of valid children or data
    //! pointers
    unsigned short slotuse;

    //! Delayed initialisation of constructed node.
    inline void initialize(unsigned short l) noexcept {
        level = l;
        slotuse = 0;
    }

    //! True if this is a leaf node.
    inline bool is_leafnode() const noexcept {
        return (level == 0);
    }
};

//! Extended structure of a inner node in-memory. Contains only keys and no
//! data items.
template <typename Key, std::size_t SlotMin, std::size_t SlotMax>
struct inner_node : public node {
    using key_type = Key;

    std::size_t subtree_size;
    std::int64_t delay;

    //! Keys of children or data pointers
    key_type slotkey[SlotMax];  // NOLINT

    //! Pointers to children
    node* childid[SlotMax + 1];  // NOLINT

    //! Set variables to initial values.
    inline void initialize(std::int64_t d, unsigned short l) noexcept {
        subtree_size = 0;
        delay = d;
        node::initialize(l);
    }

    //! Return key in slot s
    inline key_type const& key(size_t s) const {
        return slotkey[s];
    }

    //! True if the node's slots are full.
    inline bool is_full() const noexcept {
        return (node::slotuse == SlotMax);
    }

    //! True if few used entries, less than half full.
    inline bool is_few() const noexcept {
        return (node::slotuse <= SlotMin);
    }

    //! True if node has too few entries.
    inline bool is_underflow() const noexcept {
        return (node::slotuse < SlotMin);
    }
};

//! Extended structure of a leaf node in memory. Contains pairs of keys and
//! data items. Key and data slots are kept together in value_type.
template <typename Key, typename Value, typename KeyOfValue, std::size_t SlotMin, std::size_t SlotMax>
struct leaf_node : public node {
    using key_type = Key;
    using value_type = Value;
    //! Double linked list pointers to traverse the leaves
    leaf_node* prev_leaf;

    //! Double linked list pointers to traverse the leaves
    leaf_node* next_leaf;

    //! Array of (key, data) pairs
    value_type slotdata[SlotMax];  // NOLINT
    std::int64_t delays[SlotMax];

    //! Set variables to initial values
    inline void initialize() noexcept {
        node::initialize(0);
        prev_leaf = next_leaf = nullptr;
    }

    //! Return key in slot s
    inline key_type const& key(size_t s) const {
        return KeyOfValue::get(slotdata[s]);
    }

    //! True if the node's slots are full.
    inline bool is_full() const noexcept {
        return (node::slotuse == SlotMax);
    }

    //! True if few used entries, less than half full.
    inline bool is_few() const noexcept {
        return (node::slotuse <= SlotMin);
    }

    //! True if node has too few entries.
    inline bool is_underflow() const noexcept {
        return (node::slotuse < SlotMin);
    }
};

/*!
 * Basic class implementing a tree data structure in memory.
 *
 * The base implementation of an in-memory tree. It is based on the
 * implementation in Cormen's Introduction into Algorithms, Jan Jannink's paper
 * and other algorithm resources. Almost all STL-required function calls are
 * implemented. The asymptotic time requirements of the STL are not always
 * fulfilled in theory, however, in practice this tree performs better than a
 * red-black tree and almost always uses less memory. The insertion function
 * splits the nodes on the recursion unroll. Erase is largely based on Jannink's
 * ideas.
 *
 * This class is specialized into btree_set, btree_multiset, btree_map and
 * btree_multimap using default template parameters and facade functions.
 */
template <typename Key, typename Value, typename KeyOfValue, typename Compare = std::less<Key>,
          typename Traits = btree_default_traits<Key, Value>, typename Allocator = std::allocator<char>>
class ReplayTree {
   public:
    //! \name Template Parameter Types
    //! \{

    //! First template parameter: The key type of the tree. This is stored in
    //! inner nodes.
    using key_type = Key;

    //! Second template parameter: Composition pair of key and data types, or
    //! just the key for set containers. This data type is stored in the leaves.
    using value_type = Value;

    //! Third template: key extractor class to pull key_type from value_type.
    using key_of_value = KeyOfValue;

    //! Fourth template parameter: key_type comparison function object
    using key_compare = Compare;

    //! Fifth template parameter: Traits object used to define more parameters
    //! of the tree
    using traits = Traits;

    //! Seventh template parameter: STL allocator for tree nodes
    using allocator_type = Allocator;

    //! \}
   private:
    using alloc_traits = std::allocator_traits<allocator_type>;

   public:
    //! \name Constructed Types
    //! \{

    //! Typedef of our own type
    using self_type = ReplayTree<key_type, value_type, key_of_value, key_compare, traits, allocator_type>;

    //! Size type used to count keys
    using size_type = typename alloc_traits::size_type;

    //! \}

   public:
    //! \name Static Constant Options and Values of the Tree
    //! \{

    //! Base tree parameter: The number of key/data slots in each leaf
    static constexpr unsigned short leaf_slotmax = traits::leaf_slots;

    //! Base tree parameter: The number of key slots in each inner node,
    //! this can differ from slots in each leaf.
    static constexpr unsigned short inner_slotmax = traits::inner_slots;

    //! Computed tree parameter: The minimum number of key/data slots used
    //! in a leaf. If fewer slots are used, the leaf will be merged or slots
    //! shifted from it's siblings.
    static constexpr unsigned short leaf_slotmin = (leaf_slotmax / 2);

    //! Computed tree parameter: The minimum number of key slots used
    //! in an inner node. If fewer slots are used, the inner node will be
    //! merged or slots shifted from it's siblings.
    static constexpr unsigned short inner_slotmin = (inner_slotmax / 2);

    //! Debug parameter: Enables expensive and thorough checking of the tree
    //! invariants after each insert/erase operation.
    static constexpr bool self_verify = traits::self_verify;

    //! Debug parameter: Prints out lots of debug information about how the
    //! algorithms change the tree. Requires the header file to be compiled
    //! with REPLAY_TREE_DEBUG and the key type must be std::ostream printable.
    static constexpr bool debug = traits::debug;

    //! \}

   public:
    //! \name Small Statistics Structure
    //! \{

    /*!
     * A small struct containing basic statistics about the tree. It can be
     * fetched using get_stats().
     */
    struct tree_stats {
        //! Number of items in the tree
        size_type size;

        //! Number of leaves in the tree
        size_type leaves;

        //! Number of inner nodes in the tree
        size_type inner_nodes;

        //! Base tree parameter: The number of key/data slots in each leaf
        static constexpr unsigned short leaf_slots = self_type::leaf_slotmax;

        //! Base tree parameter: The number of key slots in each inner node.
        static constexpr unsigned short inner_slots = self_type::inner_slotmax;

        //! Zero initialized
        tree_stats() : size(0), leaves(0), inner_nodes(0) {
        }

        //! Return the total number of nodes
        size_type nodes() const {
            return inner_nodes + leaves;
        }

        //! Return the average fill of leaves
        double avgfill_leaves() const {
            return static_cast<double>(size) / (leaves * leaf_slots);
        }
    };

    //! \}

   private:
    using InnerNode = inner_node<key_type, inner_slotmin, inner_slotmax>;
    using LeafNode = leaf_node<key_type, value_type, KeyOfValue, leaf_slotmin, leaf_slotmax>;

   public:
    using iterator = TreeIterator<LeafNode, false>;
    using const_iterator = TreeIterator<LeafNode, true>;
    using reverse_iterator = std::reverse_iterator<iterator>;
    using const_reverse_iterator = std::reverse_iterator<const_iterator>;

   private:
    //! \name Tree Object Data Members
    //! \{

    //! Pointer to the tree's root node, either leaf or inner node.
    node* root_;

    //! Pointer to first leaf in the double linked leaf chain.
    LeafNode* head_leaf_;

    //! Pointer to last leaf in the double linked leaf chain.
    LeafNode* tail_leaf_;

    //! Other small statistics about the tree.
    tree_stats stats_;

    //! Key comparison object. More comparison functions are generated from
    //! this < relation.
    key_compare key_less_;

    using inner_node_alloc_traits = typename alloc_traits::template rebind_traits<InnerNode>;
    using inner_node_alloc_type = typename inner_node_alloc_traits::allocator_type;
    using leaf_node_alloc_traits = typename alloc_traits::template rebind_traits<LeafNode>;
    using leaf_node_alloc_type = typename leaf_node_alloc_traits::allocator_type;

    //! Memory allocator.
    inner_node_alloc_type inner_node_allocator_;
    leaf_node_alloc_type leaf_node_allocator_;

    //! \}

   public:
    //! \name Constructors and Destructor
    //! \{

    //! Default constructor initializing an empty tree with the standard key
    //! comparison function.
    explicit ReplayTree(allocator_type const& alloc = allocator_type())
        : root_(nullptr),
          head_leaf_(nullptr),
          tail_leaf_(nullptr),
          inner_node_allocator_(alloc),
          leaf_node_allocator_(alloc) {
    }

    //! Constructor initializing an empty tree with a special key
    //! comparison object.
    explicit ReplayTree(const key_compare& kcf, const allocator_type& alloc = allocator_type())
        : root_(nullptr),
          head_leaf_(nullptr),
          tail_leaf_(nullptr),
          key_less_(kcf),
          inner_node_allocator_(alloc),
          leaf_node_allocator_(alloc) {
    }

    //! Constructor initializing a tree with the range [first,last). The
    //! range need not be sorted. To create a tree from a sorted range, use
    //! bulk_load().
    template <class InputIterator>
    ReplayTree(InputIterator first, InputIterator last, const allocator_type& alloc = allocator_type())
        : root_(nullptr),
          head_leaf_(nullptr),
          tail_leaf_(nullptr),
          inner_node_allocator_(alloc),
          leaf_node_allocator_(alloc) {
        insert(first, last);
    }

    //! Constructor initializing a tree with the range [first,last) and a
    //! special key comparison object.  The range need not be sorted. To create
    //! a tree from a sorted range, use bulk_load().
    template <class InputIterator>
    ReplayTree(InputIterator first, InputIterator last, key_compare const& kcf,
               const allocator_type& alloc = allocator_type())
        : root_(nullptr),
          head_leaf_(nullptr),
          tail_leaf_(nullptr),
          key_less_(kcf),
          inner_node_allocator_(alloc),
          leaf_node_allocator_(alloc) {
        insert(first, last);
    }

    //! Frees up all used tree memory pages
    ~ReplayTree() {
        clear();
    }

    //! Fast swapping of two identical tree objects.
    void swap(ReplayTree& from) {
        std::swap(root_, from.root_);
        std::swap(head_leaf_, from.head_leaf_);
        std::swap(tail_leaf_, from.tail_leaf_);
        std::swap(stats_, from.stats_);
        std::swap(key_less_, from.key_less_);
        std::swap(inner_node_allocator_, from.inner_node_allocator_);
        std::swap(leaf_node_allocator_, from.leaf_node_allocator_);
    }

    //! \}

   public:
    //! \name Key and Value Comparison Function Objects
    //! \{

    //! Function class to compare value_type objects. Required by the STL
    class value_compare {
       protected:
        //! Key comparison function from the template parameter
        key_compare key_comp;

        //! Constructor called from ReplayTree::value_comp()
        explicit value_compare(key_compare kc) : key_comp(kc) {
        }

        //! Friendly to the btree class so it may call the constructor
        friend class ReplayTree<key_type, value_type, key_of_value, key_compare, traits, allocator_type>;

       public:
        //! Function call "less"-operator resulting in true if x < y.
        inline bool operator()(value_type const& x, value_type const& y) const {
            return key_comp(x.first, y.first);
        }
    };

    //! Constant access to the key comparison object sorting the tree.
    key_compare key_comp() const {
        return key_less_;
    }

    //! Constant access to a constructed value_type comparison object. Required
    //! by the STL.
    value_compare value_comp() const {
        return value_compare(key_less_);
    }

    //! \}

   private:
    //! \name Convenient Key Comparison Functions Generated From key_less
    //! \{

    //! True if a < b ? "constructed" from key_less_()
    inline bool key_less(key_type const& a, key_type const& b) const {
        return key_less_(a, b);
    }

    //! True if a <= b ? constructed from key_less()
    inline bool key_lessequal(key_type const& a, key_type const& b) const {
        return !key_less_(b, a);
    }

    //! True if a > b ? constructed from key_less()
    inline bool key_greater(key_type const& a, key_type const& b) const {
        return key_less_(b, a);
    }

    //! True if a >= b ? constructed from key_less()
    inline bool key_greaterequal(key_type const& a, key_type const& b) const {
        return !key_less_(a, b);
    }

    //! True if a == b ? constructed from key_less(). This requires the <
    //! relation to be a total order, otherwise the tree cannot be sorted.
    bool key_equal(const key_type& a, const key_type& b) const {
        return !key_less_(a, b) && !key_less_(b, a);
    }

    static inline std::size_t get_subtree_size(node const* n) {
        if (n->is_leafnode()) {
            return static_cast<LeafNode const*>(n)->slotuse;
        } else {
            return static_cast<InnerNode const*>(n)->subtree_size;
        }
    }

    static inline void update_subtree_size(InnerNode* n) {
        n->subtree_size = 0;
        for (unsigned short i = 0; i <= n->slotuse; ++i) {
            n->subtree_size += get_subtree_size(n->childid[i]);
        }
    }

    static inline void add_delay(node* n, std::int64_t d) {
        if (n->is_leafnode()) {
            auto leaf = static_cast<LeafNode*>(n);
            for (std::size_t i = 0; i < leaf->slotuse; ++i) {
                leaf->delays[i] += d;
            }
        } else {
            static_cast<InnerNode*>(n)->delay += d;
        }
    }

    //! \}

   private:
    //! \name Node Object Allocation and Deallocation Functions
    //! \{

    //! Allocate and initialize a leaf node
    LeafNode* allocate_leaf() {
        auto ptr = leaf_node_alloc_traits::allocate(leaf_node_allocator_, 1);
        LeafNode* n = new (ptr) LeafNode();
        n->initialize();
        ++stats_.leaves;
        return n;
    }

    //! Allocate and initialize an inner node
    InnerNode* allocate_inner(std::int64_t delay, unsigned short level) {
        auto ptr = inner_node_alloc_traits::allocate(inner_node_allocator_, 1);
        InnerNode* n = new (ptr) InnerNode();
        n->initialize(delay, level);
        ++stats_.inner_nodes;
        return n;
    }

    //! Correctly free either inner or leaf node, destructs all contained key
    //! and value objects.
    void free_node(node* n) {
        if (n->is_leafnode()) {
            LeafNode* ln = static_cast<LeafNode*>(n);
            leaf_node_alloc_traits::destroy(leaf_node_allocator_, ln);
            leaf_node_alloc_traits::deallocate(leaf_node_allocator_, ln, 1);
            --stats_.leaves;
        } else {
            InnerNode* in = static_cast<InnerNode*>(n);
            inner_node_alloc_traits::destroy(inner_node_allocator_, in);
            inner_node_alloc_traits::deallocate(inner_node_allocator_, in, 1);
            --stats_.inner_nodes;
        }
    }

    //! \}

   public:
    //! \name Fast Destruction of the Tree
    //! \{

    //! Frees all key/data pairs and all nodes of the tree.
    void clear() {
        if (root_) {
            clear_recursive(root_);
            free_node(root_);

            root_ = nullptr;
            head_leaf_ = tail_leaf_ = nullptr;

            stats_ = tree_stats();
        }

        REPLAY_TREE_ASSERT(stats_.size == 0);
    }

   private:
    //! Recursively free up nodes.
    void clear_recursive(node* n) {
        if (n->is_leafnode()) {
            LeafNode* leafnode = static_cast<LeafNode*>(n);

            for (unsigned short slot = 0; slot < leafnode->slotuse; ++slot) {
                // data objects are deleted by LeafNode's destructor
            }
        } else {
            InnerNode* innernode = static_cast<InnerNode*>(n);

            for (unsigned short slot = 0; slot < innernode->slotuse + 1; ++slot) {
                clear_recursive(innernode->childid[slot]);
                free_node(innernode->childid[slot]);
            }
        }
    }

    //! \}

   public:
    //! \name STL Iterator Construction Functions
    //! \{

    //! Constructs a read/data-write iterator that points to the first slot in
    //! the first leaf of the tree.
    inline iterator begin() {
        return iterator(head_leaf_, 0);
    }

    //! Constructs a read/data-write iterator that points to the first invalid
    //! slot in the last leaf of the tree.
    inline iterator end() {
        return iterator(tail_leaf_, tail_leaf_ ? tail_leaf_->slotuse : 0);
    }

    //! Constructs a read-only constant iterator that points to the first slot
    //! in the first leaf of the tree.
    inline const_iterator begin() const {
        return const_iterator(head_leaf_, 0);
    }

    //! Constructs a read-only constant iterator that points to the first
    //! invalid slot in the last leaf of the tree.
    inline const_iterator end() const {
        return const_iterator(tail_leaf_, tail_leaf_ ? tail_leaf_->slotuse : 0);
    }

    //! Constructs a read/data-write reverse iterator that points to the first
    //! invalid slot in the last leaf of the tree. Uses STL magic.
    inline reverse_iterator rbegin() {
        return reverse_iterator(end());
    }

    //! Constructs a read/data-write reverse iterator that points to the first
    //! slot in the first leaf of the tree. Uses STL magic.
    inline reverse_iterator rend() {
        return reverse_iterator(begin());
    }

    //! Constructs a read-only reverse iterator that points to the first
    //! invalid slot in the last leaf of the tree. Uses STL magic.
    inline const_reverse_iterator rbegin() const {
        return const_reverse_iterator(end());
    }

    //! Constructs a read-only reverse iterator that points to the first slot
    //! in the first leaf of the tree. Uses STL magic.
    inline const_reverse_iterator rend() const {
        return const_reverse_iterator(begin());
    }

    //! \}

   private:
    //! \name Tree Node Binary Search Functions
    //! \{

    //! Searches for the first key in the node n greater or equal to key. Uses
    //! binary search with an optional linear self-verification. This is a
    //! template function, because the slotkey array is located at different
    //! places in LeafNode and InnerNode.
    template <typename node_type>
    unsigned short find_lower(node_type const* n, key_type const& key) const {
        if constexpr (sizeof(*n) > traits::binsearch_threshold) {
            if (n->slotuse == 0)
                return 0;

            unsigned short lo = 0, hi = n->slotuse;

            while (lo < hi) {
                auto mid = static_cast<unsigned short>((lo + hi) >> 1u);

                if (key_lessequal(key, n->key(mid))) {
                    hi = mid;  // key <= mid
                } else {
                    lo = mid + 1;  // key > mid
                }
            }

            // verify result using simple linear search
            if (self_verify) {
                unsigned short i = 0;
                while (i < n->slotuse && key_less(n->key(i), key))
                    ++i;

                REPLAY_TREE_ASSERT(i == lo);
            }

            return lo;
        } else  // for nodes <= binsearch_threshold do linear search.
        {
            unsigned short lo = 0;
            while (lo < n->slotuse && key_less(n->key(lo), key))
                ++lo;
            return lo;
        }
    }

    //! Searches for the first key in the node n greater than key. Uses binary
    //! search with an optional linear self-verification. This is a template
    //! function, because the slotkey array is located at different places in
    //! LeafNode and InnerNode.
    template <typename node_type>
    unsigned short find_upper(const node_type* n, const key_type& key) const {
        if constexpr (sizeof(*n) > traits::binsearch_threshold) {
            if (n->slotuse == 0)
                return 0;

            unsigned short lo = 0, hi = n->slotuse;

            while (lo < hi) {
                auto mid = static_cast<unsigned short>((lo + hi) >> 1u);

                if (key_less(key, n->key(mid))) {
                    hi = mid;  // key < mid
                } else {
                    lo = mid + 1;  // key >= mid
                }
            }

            // verify result using simple linear search
            if (self_verify) {
                unsigned short i = 0;
                while (i < n->slotuse && key_lessequal(n->key(i), key))
                    ++i;

                REPLAY_TREE_ASSERT(i == hi);
            }

            return lo;
        } else  // for nodes <= binsearch_threshold do linear search.
        {
            unsigned short lo = 0;
            while (lo < n->slotuse && key_lessequal(n->key(lo), key))
                ++lo;
            return lo;
        }
    }

    //! \}

   public:
    //! \name Access Functions to the Item Count
    //! \{

    //! Return the number of key/data pairs in the tree
    inline size_type size() const noexcept {
        return stats_.size;
    }

    //! Returns true if there is at least one key/data pair in the tree
    inline bool empty() const noexcept {
        return (size() == size_type(0));
    }

    //! Returns the largest possible size of the tree. This is just a
    //! function required by the STL standard, the Tree can hold more items.
    constexpr size_type max_size() const noexcept {
        return std::numeric_limits<size_type>::max();
    }

    //! Return a const reference to the current statistics.
    inline tree_stats const& get_stats() const {
        return stats_;
    }

    //! \}

   public:
    //! \name STL Access Functions Querying the Tree by Descending to a Leaf
    //! \{

    //! Non-STL function checking whether a key is in the tree. The same as
    //! (find(k) != end()) or (count() != 0).
    bool exists(key_type const& key) const {
        node const* n = root_;
        if (!n)
            return false;

        while (!n->is_leafnode()) {
            auto inner = static_cast<InnerNode const*>(n);
            unsigned short slot = find_lower(inner, key);

            n = inner->childid[slot];
        }

        auto leaf = static_cast<LeafNode const*>(n);

        unsigned short slot = find_lower(leaf, key);
        return (slot < leaf->slotuse && key_equal(key, leaf->key(slot)));
    }

    //! Tries to locate a key in the tree and returns an iterator to the
    //! key/data slot if found. If unsuccessful it returns end().
    iterator find(key_type const& key) {
        node* n = root_;
        if (!n)
            return end();

        while (!n->is_leafnode()) {
            auto inner = static_cast<InnerNode const*>(n);
            unsigned short slot = find_lower(inner, key);

            n = inner->childid[slot];
        }

        auto leaf = static_cast<LeafNode*>(n);

        unsigned short slot = find_lower(leaf, key);
        return (slot < leaf->slotuse && key_equal(key, leaf->key(slot))) ? iterator(leaf, slot) : end();
    }

    //! Tries to locate a key in the tree and returns an constant iterator to
    //! the key/data slot if found. If unsuccessful it returns end().
    const_iterator find(key_type const& key) const {
        const_iterator(const_cast<self_type*>(this)->find(key));
    }

    //! Tries to locate a key in the tree and returns the number of identical
    //! key entries found.
    size_type count(key_type const& key) const {
        if (exists(key)) {
            return 1;
        } else {
            return 0;
        }
    }

    //! Searches the tree and returns an iterator to the first pair equal to
    //! or greater than key, or end() if all keys are smaller.
    iterator lower_bound(key_type const& key) {
        node* n = root_;
        if (!n)
            return end();

        while (!n->is_leafnode()) {
            auto inner = static_cast<InnerNode const*>(n);
            unsigned short slot = find_lower(inner, key);

            n = inner->childid[slot];
        }

        LeafNode* leaf = static_cast<LeafNode*>(n);

        unsigned short slot = find_lower(leaf, key);
        return iterator(leaf, slot);
    }

    //! Searches the tree and returns a constant iterator to the first pair
    //! equal to or greater than key, or end() if all keys are smaller.
    const_iterator lower_bound(key_type const& key) const {
        return const_iterator(const_cast<self_type*>(this)->lower_bound(key));
    }

    //! Searches the tree and returns an iterator to the first pair greater
    //! than key, or end() if all keys are smaller or equal.
    iterator upper_bound(key_type const& key) {
        node* n = root_;
        if (!n)
            return end();

        while (!n->is_leafnode()) {
            auto inner = static_cast<InnerNode const*>(n);
            unsigned short slot = find_upper(inner, key);

            n = inner->childid[slot];
        }

        auto leaf = static_cast<LeafNode*>(n);

        unsigned short slot = find_upper(leaf, key);
        return iterator(leaf, slot);
    }

    //! Searches the tree and returns a constant iterator to the first pair
    //! greater than key, or end() if all keys are smaller or equal.
    const_iterator upper_bound(key_type const& key) const {
        return const_iterator(const_cast<self_type*>(this)->upper_bound(key));
    }

    //! Searches the tree and returns both lower_bound() and upper_bound().
    std::pair<iterator, iterator> equal_range(const key_type& key) {
        return std::pair<iterator, iterator>(lower_bound(key), upper_bound(key));
    }

    //! Searches the tree and returns both lower_bound() and upper_bound().
    std::pair<const_iterator, const_iterator> equal_range(const key_type& key) const {
        return std::pair<const_iterator, const_iterator>(lower_bound(key), upper_bound(key));
    }

    std::size_t get_rank(key_type const& key) const {
        node* n = root_;
        if (!n)
            return 0;
        std::size_t rank = 0;

        while (!n->is_leafnode()) {
            auto inner = static_cast<InnerNode const*>(n);
            unsigned short slot = find_lower(inner, key);
            for (std::size_t i = 0; i < slot; ++i) {
                rank += get_subtree_size(inner->childid[i]);
            }

            n = inner->childid[slot];
        }

        auto leaf = static_cast<LeafNode*>(n);

        rank += find_lower(leaf, key);
        return rank;
    }

    void increase_delay(key_type const& key) {
        node* n = root_;
        if (!n)
            return;
        while (!n->is_leafnode()) {
            auto inner = static_cast<InnerNode const*>(n);
            unsigned short slot = find_lower(inner, key);
            for (std::size_t i = 0; i < slot; ++i) {
                add_delay(inner->childid[i], 1);
            }

            n = inner->childid[slot];
        }

        auto leaf = static_cast<LeafNode*>(n);

        auto slot = find_lower(leaf, key);
        for (std::size_t i = 0; i < slot; ++i) {
            ++leaf->delays[i];
        }
    }

    void increase_global_delay() {
        node* n = root_;
        if (!n)
            return;
        add_delay(root_, 1);
    }

    //! \}
   public:
    //! \name Fast Copy: Assign Operator and Copy Constructors
    //! \{

    //! Assignment operator. All the key/data pairs are copied.
    ReplayTree& operator=(ReplayTree const& other) {
        if (this != &other) {
            clear();

            key_less_ = other.key_comp();
            inner_node_allocator_ = other.inner_node_allocator_;
            leaf_node_allocator_ = other.leaf_node_allocator_;

            if (other.size() != 0) {
                stats_.leaves = stats_.inner_nodes = 0;
                if (other.root_) {
                    root_ = copy_recursive(other.root_);
                }
                stats_ = other.stats_;
            }

            if (self_verify)
                verify();
        }
        return *this;
    }

    //! Copy constructor. The newly initialized tree object will contain a
    //! copy of all key/data pairs.
    ReplayTree(ReplayTree const& other)
        : root_(nullptr),
          head_leaf_(nullptr),
          tail_leaf_(nullptr),
          stats_(other.stats_),
          key_less_(other.key_comp()),
          inner_node_allocator_(other.inner_node_allocator_),
          leaf_node_allocator_(other.leaf_node_allocator_) {
        if (size() > 0) {
            stats_.leaves = stats_.inner_nodes = 0;
            if (other.root_) {
                root_ = copy_recursive(other.root_);
            }
            if (self_verify)
                verify();
        }
    }

   private:
    //! Recursively copy nodes from another tree object
    node* copy_recursive(node const* n) {
        if (n->is_leafnode()) {
            auto leaf = static_cast<LeafNode const*>(n);
            auto newleaf = allocate_leaf();

            newleaf->slotuse = leaf->slotuse;
            std::copy(leaf->slotdata, leaf->slotdata + leaf->slotuse, newleaf->slotdata);
            std::copy(leaf->delays, leaf->delays + leaf->slotuse, newleaf->delays);

            if (head_leaf_ == nullptr) {
                head_leaf_ = tail_leaf_ = newleaf;
                newleaf->prev_leaf = newleaf->next_leaf = nullptr;
            } else {
                newleaf->prev_leaf = tail_leaf_;
                tail_leaf_->next_leaf = newleaf;
                tail_leaf_ = newleaf;
            }

            return newleaf;
        } else {
            auto inner = static_cast<InnerNode const*>(n);
            auto newinner = allocate_inner(inner->delay, inner->level);

            newinner->slotuse = inner->slotuse;
            std::copy(inner->slotkey, inner->slotkey + inner->slotuse, newinner->slotkey);

            for (unsigned short slot = 0; slot <= inner->slotuse; ++slot) {
                newinner->childid[slot] = copy_recursive(inner->childid[slot]);
            }

            newinner->subtree_size = inner->subtree_size;

            return newinner;
        }
    }

    //! \}

   public:
    //! \name Public Insertion Functions
    //! \{

    //! Attempt to insert a key/data pair into the tree. If the tree does not
    //! allow duplicate keys, then the insert may fail if it is already present.
    iterator insert(value_type const& x) {
        return insert_start(key_of_value::get(x), x);
    }

    //! Attempt to insert the range [first,last) of value_type pairs into the B+
    //! tree. Each key/data pair is inserted individually; to bulk load the
    //! tree, use a constructor with range.
    template <typename InputIterator>
    void insert(InputIterator first, InputIterator last) {
        InputIterator iter = first;
        while (iter != last) {
            insert(*iter);
            ++iter;
        }
    }

    //! \}

   private:
    //! \name Private Insertion Functions
    //! \{

    //! Start the insertion descent at the current root and handle root splits.
    //! Returns true if the item was inserted
    iterator insert_start(key_type const& key, value_type const& value) {
        node* newchild = nullptr;
        key_type newkey = key_type();

        if (root_ == nullptr) {
            root_ = head_leaf_ = tail_leaf_ = allocate_leaf();
        }

        auto r = insert_descend(root_, key, value, 0, &newkey, &newchild);

        if (newchild) {
            // this only occurs if insert_descend() could not insert the key
            // into the root node, this mean the root is full and a new root
            // needs to be created.
            InnerNode* newroot = allocate_inner(0, root_->level + 1);
            newroot->slotkey[0] = newkey;

            newroot->childid[0] = root_;
            newroot->childid[1] = newchild;
            newroot->slotuse = 1;
            update_subtree_size(newroot);

            root_ = newroot;
        }

        ++stats_.size;

#ifdef REPLAY_TREE_DEBUG
        if (debug)
            print(std::cout);
#endif

        if (self_verify) {
            verify();
            REPLAY_TREE_ASSERT(exists(key));
        }

        return r;
    }

    /*!
     * Insert an item into the tree.
     *
     * Descend down the nodes to a leaf, insert the key/data pair in a free
     * slot. If the node overflows, then it must be split and the new split node
     * inserted into the parent. Unroll / this splitting up to the root.
     */
    iterator insert_descend(node* n, key_type const& key, value_type const& value, std::int64_t delay,
                            key_type* splitkey, node** splitnode) {
        if (!n->is_leafnode()) {
            auto inner = static_cast<InnerNode*>(n);

            key_type newkey = key_type();
            node* newchild = nullptr;

            unsigned short slot = find_lower(inner, key);

            auto r = insert_descend(inner->childid[slot], key, value, delay + inner->delay, &newkey, &newchild);

            if (newchild) {
                if (inner->is_full()) {
                    split_inner_node(inner, splitkey, splitnode, slot);

                    // check if insert slot is in the split sibling node
                    if (slot == inner->slotuse + 1 && inner->slotuse < (*splitnode)->slotuse) {
                        // special case when the insert slot matches the split
                        // place between the two nodes, then the insert key
                        // becomes the split key.

                        REPLAY_TREE_ASSERT(inner->slotuse + 1 < inner_slotmax);

                        auto split = static_cast<InnerNode*>(*splitnode);

                        // move the split key and it's datum into the left node
                        inner->slotkey[inner->slotuse] = *splitkey;
                        inner->childid[inner->slotuse + 1] = split->childid[0];
                        ++inner->slotuse;

                        // set new split key and move corresponding datum into
                        // right node

                        split->childid[0] = newchild;
                        *splitkey = newkey;

                        update_subtree_size(inner);
                        update_subtree_size(split);

                        return r;
                    } else if (slot >= inner->slotuse + 1) {
                        // in case the insert slot is in the newly create split
                        // node, we reuse the code below.

                        slot -= static_cast<unsigned short>(inner->slotuse + 1u);
                        inner = static_cast<InnerNode*>(*splitnode);
                    }
                }

                // move items and put pointer to child node into correct slot
                REPLAY_TREE_ASSERT(slot >= 0 && slot <= inner->slotuse);

                std::copy_backward(inner->slotkey + slot, inner->slotkey + inner->slotuse,
                                   inner->slotkey + inner->slotuse + 1);
                std::copy_backward(inner->childid + slot, inner->childid + inner->slotuse + 1,
                                   inner->childid + inner->slotuse + 2);

                inner->slotkey[slot] = newkey;
                inner->childid[slot + 1] = newchild;

                ++inner->slotuse;
            }
            update_subtree_size(inner);
            return r;
        } else  // n->is_leafnode() == true
        {
            auto leaf = static_cast<LeafNode*>(n);

            unsigned short slot = find_lower(leaf, key);

            if (leaf->is_full()) {
                split_leaf_node(leaf, splitkey, splitnode);

                // check if insert slot is in the split sibling node
                if (slot >= leaf->slotuse) {
                    slot -= leaf->slotuse;
                    leaf = static_cast<LeafNode*>(*splitnode);
                }
            }

            // move items and put data item into correct data slot
            REPLAY_TREE_ASSERT(slot >= 0 && slot <= leaf->slotuse);

            std::copy_backward(leaf->slotdata + slot, leaf->slotdata + leaf->slotuse,
                               leaf->slotdata + leaf->slotuse + 1);
            std::copy_backward(leaf->delays + slot, leaf->delays + leaf->slotuse, leaf->delays + leaf->slotuse + 1);

            leaf->slotdata[slot] = value;
            leaf->delays[slot] = -delay;
            ++leaf->slotuse;

            if (splitnode && leaf != *splitnode && slot == leaf->slotuse - 1) {
                // special case: the node was split, and the insert is at the
                // last slot of the old node. then the splitkey must be updated.
                *splitkey = key;
            }

            return iterator(leaf, slot);
        }
    }

    //! Split up a leaf node into two equally-filled sibling leaves. Returns the
    //! new nodes and it's insertion key in the two parameters.
    void split_leaf_node(LeafNode* leaf, key_type* out_newkey, node** out_newleaf) {
        REPLAY_TREE_ASSERT(leaf->is_full());

        unsigned short mid = (leaf->slotuse >> 1);

        LeafNode* newleaf = allocate_leaf();

        newleaf->slotuse = leaf->slotuse - mid;

        newleaf->next_leaf = leaf->next_leaf;
        if (newleaf->next_leaf == nullptr) {
            REPLAY_TREE_ASSERT(leaf == tail_leaf_);
            tail_leaf_ = newleaf;
        } else {
            newleaf->next_leaf->prev_leaf = newleaf;
        }

        std::copy(leaf->slotdata + mid, leaf->slotdata + leaf->slotuse, newleaf->slotdata);
        std::copy(leaf->delays + mid, leaf->delays + leaf->slotuse, newleaf->delays);

        leaf->slotuse = mid;
        leaf->next_leaf = newleaf;
        newleaf->prev_leaf = leaf;

        *out_newkey = leaf->key(leaf->slotuse - 1);
        *out_newleaf = newleaf;
    }

    //! Split up an inner node into two equally-filled sibling nodes. Returns
    //! the new nodes and it's insertion key in the two parameters. Requires the
    //! slot of the item will be inserted, so the nodes will be the same size
    //! after the insert.
    void split_inner_node(InnerNode* inner, key_type* out_newkey, node** out_newinner, unsigned int addslot) {
        REPLAY_TREE_ASSERT(inner->is_full());

        unsigned short mid = (inner->slotuse >> 1);

        // if the split is uneven and the overflowing item will be put into the
        // larger node, then the smaller split node may underflow
        if (addslot <= mid && mid > inner->slotuse - (mid + 1))
            mid--;

        InnerNode* newinner = allocate_inner(inner->delay, inner->level);

        newinner->slotuse = inner->slotuse - static_cast<unsigned short>(mid + 1u);

        std::copy(inner->slotkey + mid + 1, inner->slotkey + inner->slotuse, newinner->slotkey);
        std::copy(inner->childid + mid + 1, inner->childid + inner->slotuse + 1, newinner->childid);

        inner->slotuse = mid;

        update_subtree_size(inner);
        update_subtree_size(newinner);

        *out_newkey = inner->key(mid);
        *out_newinner = newinner;
    }

    //! \}

   private:
    //! \name Support Class Encapsulating Deletion Results
    //! \{

    //! Result flags of recursive deletion.
    enum result_flags_t {
        //! Deletion successful and no fix-ups necessary.
        btree_ok = 0,

        //! Deletion not successful because key was not found.
        btree_not_found = 1,

        //! Deletion successful, the last key was updated so parent slotkeys
        //! need updates.
        btree_update_lastkey = 2,

        //! Deletion successful, children nodes were merged and the parent needs
        //! to remove the empty node.
        btree_fixmerge = 4
    };

    //! tree recursive deletion has much information which is needs to be
    //! passed upward.
    struct result_t {
        //! Merged result flags
        result_flags_t flags;

        //! The key to be updated at the parent's slot
        key_type lastkey;

        //! Constructor of a result with a specific flag, this can also be used
        //! as for implicit conversion.
        result_t(result_flags_t f = btree_ok)  // NOLINT
            : flags(f), lastkey() {
        }

        //! Constructor with a lastkey value.
        result_t(result_flags_t f, const key_type& k) : flags(f), lastkey(k) {
        }

        //! Test if this result object has a given flag set.
        bool has(result_flags_t f) const {
            return (flags & f) != 0;
        }

        //! Merge two results OR-ing the result flags and overwriting lastkeys.
        result_t& operator|=(const result_t& other) {
            flags = result_flags_t(flags | other.flags);

            // we overwrite existing lastkeys on purpose
            if (other.has(btree_update_lastkey))
                lastkey = other.lastkey;

            return *this;
        }
    };

    //! \}

   public:
    //! \name Public Erase Functions
    //! \{

    //! Erase the key/data pair referenced by the iterator.
    std::pair<bool, std::int64_t> erase(iterator iter) {
        if (self_verify)
            verify();

        if (!root_)
            return {false, 0};

        auto [result, delay] = erase_iter_descend(iter, root_, 0, nullptr, nullptr, nullptr, nullptr, nullptr, 0, 0, 0);
        bool success = false;

        if (!result.has(btree_not_found)) {
            --stats_.size;
            success = true;
        }

#ifdef REPLAY_TREE_DEBUG
        if (debug)
            print(std::cout);
#endif
        if (self_verify)
            verify();
        return {success, delay};
    }

   private:
    //! \name Private Erase Functions
    //! \{

    /*!
     * Erase one key/data pair referenced by an iterator in the tree.
     *
     * Descends down the tree in search of an iterator. During the descent the
     * parent, left and right siblings and their parents are computed and passed
     * down. The difficulty is that the iterator contains only a pointer to a
     * LeafNode, which means that this function must do a recursive depth first
     * search for that leaf node in the subtree containing all pairs of the same
     * key. This subtree can be very large, even the whole tree, though in
     * practice it would not make sense to have so many duplicate keys.
     *
     * Once the referenced key/data pair is found, it is removed from the leaf
     * and the same underflow cases are handled as in erase_one_descend.
     */
    std::pair<result_t, std::int64_t> erase_iter_descend(iterator const& iter, node* curr, std::int64_t delay,
                                                         node* left, node* right, InnerNode* left_parent,
                                                         InnerNode* right_parent, InnerNode* parent,
                                                         unsigned int parentslot, std::int64_t left_delay,
                                                         std::int64_t right_delay) {
        if (curr->is_leafnode()) {
            auto leaf = static_cast<LeafNode*>(curr);
            auto left_leaf = static_cast<LeafNode*>(left);
            auto right_leaf = static_cast<LeafNode*>(right);

            // if this is not the correct leaf, get next step in recursive
            // search
            if (leaf != iter.curr_leaf) {
                return {btree_not_found, 0};
            }

            if (iter.curr_slot >= leaf->slotuse) {
                return {btree_not_found, 0};
            }

            unsigned short slot = iter.curr_slot;

            auto erased_delay = leaf->delays[slot];

            std::copy(leaf->slotdata + slot + 1, leaf->slotdata + leaf->slotuse, leaf->slotdata + slot);
            std::copy(leaf->delays + slot + 1, leaf->delays + leaf->slotuse, leaf->delays + slot);

            leaf->slotuse--;

            result_t myres = btree_ok;

            // if the last key of the leaf was changed, the parent is notified
            // and updates the key of this leaf
            if (slot == leaf->slotuse) {
                if (parent && parentslot < parent->slotuse) {
                    REPLAY_TREE_ASSERT(parent->childid[parentslot] == curr);
                    parent->slotkey[parentslot] = leaf->key(leaf->slotuse - 1);
                } else {
                    if (leaf->slotuse >= 1) {
                        myres |= result_t(btree_update_lastkey, leaf->key(leaf->slotuse - 1));
                    } else {
                        REPLAY_TREE_ASSERT(leaf == root_);
                    }
                }
            }

            if (leaf->is_underflow() && !(leaf == root_ && leaf->slotuse >= 1)) {
                // determine what to do about the underflow

                // case : if this empty leaf is the root, then delete all nodes
                // and set root to nullptr.
                if (left_leaf == nullptr && right_leaf == nullptr) {
                    REPLAY_TREE_ASSERT(leaf == root_);
                    REPLAY_TREE_ASSERT(leaf->slotuse == 0);

                    free_node(root_);

                    root_ = leaf = nullptr;
                    head_leaf_ = tail_leaf_ = nullptr;

                    // will be decremented soon by insert_start()
                    REPLAY_TREE_ASSERT(stats_.size == 1);
                    REPLAY_TREE_ASSERT(stats_.leaves == 0);
                    REPLAY_TREE_ASSERT(stats_.inner_nodes == 0);

                    return {btree_ok, delay + erased_delay};
                }
                // case : if both left and right leaves would underflow in case
                // of a shift, then merging is necessary. choose the more local
                // merger with our parent
                else if ((left_leaf == nullptr || left_leaf->is_few()) &&
                         (right_leaf == nullptr || right_leaf->is_few())) {
                    if (left_parent == parent)
                        myres |= merge_leaves(left_leaf, leaf, left_parent, left_delay, delay);
                    else
                        myres |= merge_leaves(leaf, right_leaf, right_parent, delay, right_delay);
                }
                // case : the right leaf has extra data, so balance right with
                // current
                else if ((left_leaf != nullptr && left_leaf->is_few()) &&
                         (right_leaf != nullptr && !right_leaf->is_few())) {
                    if (right_parent == parent) {
                        myres |= shift_left_leaf(leaf, right_leaf, right_parent, parentslot, delay, right_delay);
                    } else {
                        myres |= merge_leaves(left_leaf, leaf, left_parent, left_delay, delay);
                    }
                }
                // case : the left leaf has extra data, so balance left with
                // current
                else if ((left_leaf != nullptr && !left_leaf->is_few()) &&
                         (right_leaf != nullptr && right_leaf->is_few())) {
                    if (left_parent == parent) {
                        shift_right_leaf(left_leaf, leaf, left_parent, parentslot - 1, left_delay, delay);
                    } else {
                        myres |= merge_leaves(leaf, right_leaf, right_parent, delay, right_delay);
                    }
                }
                // case : both the leaf and right leaves have extra data and our
                // parent, choose the leaf with more data
                else if (left_parent == right_parent) {
                    if (left_leaf->slotuse <= right_leaf->slotuse) {
                        myres |= shift_left_leaf(leaf, right_leaf, right_parent, parentslot, delay, right_delay);
                    } else {
                        shift_right_leaf(left_leaf, leaf, left_parent, parentslot - 1, left_delay, delay);
                    }
                } else {
                    if (left_parent == parent) {
                        shift_right_leaf(left_leaf, leaf, left_parent, parentslot - 1, left_delay, delay);
                    } else {
                        myres |= shift_left_leaf(leaf, right_leaf, right_parent, parentslot, delay, right_delay);
                    }
                }
            }

            return {myres, delay + erased_delay};
        } else  // !curr->is_leafnode()
        {
            auto inner = static_cast<InnerNode*>(curr);
            auto left_inner = static_cast<InnerNode*>(left);
            auto right_inner = static_cast<InnerNode*>(right);

            // find first slot below which the searched iterator might be
            // located.

            result_t found_result;
            unsigned short slot = find_lower(inner, iter.key());

            std::int64_t found_delay;
            while (slot <= inner->slotuse) {
                node *myleft, *myright;
                InnerNode *myleft_parent, *myright_parent;
                std::int64_t myleft_delay, myright_delay;

                if (slot == 0) {
                    if (left == nullptr) {
                        myleft = nullptr;
                        myleft_delay = 0;
                    } else {
                        myleft = left_inner->childid[left->slotuse - 1];
                        myleft_delay = left_delay + left_inner->delay;
                    }
                    myleft_parent = left_inner;
                } else {
                    myleft = inner->childid[slot - 1];
                    myleft_delay = delay + inner->delay;
                    myleft_parent = inner;
                }

                if (slot == inner->slotuse) {
                    if (right == nullptr) {
                        myright = nullptr;
                        myright_delay = 0;
                    } else {
                        myright = right_inner->childid[0];
                        myright_delay = right_delay + right_inner->delay;
                    }
                    myright_parent = right_inner;
                } else {
                    myright = inner->childid[slot + 1];
                    myright_delay = delay + inner->delay;
                    myright_parent = inner;
                }

                auto [result, inner_delay] =
                    erase_iter_descend(iter, inner->childid[slot], delay + inner->delay, myleft, myright, myleft_parent,
                                       myright_parent, inner, slot, myleft_delay, myright_delay);

                if (!result.has(btree_not_found)) {
                    found_result = result;
                    found_delay = inner_delay;
                    break;
                }

                // continue recursive search for leaf on next slot

                if (slot < inner->slotuse && key_less(inner->slotkey[slot], iter.key()))
                    return {btree_not_found, 0};

                ++slot;
            }

            if (slot > inner->slotuse)
                return {btree_not_found, 0};

            update_subtree_size(inner);
            result_t myres = btree_ok;

            if (found_result.has(btree_update_lastkey)) {
                if (parent && parentslot < parent->slotuse) {
                    REPLAY_TREE_ASSERT(parent->childid[parentslot] == curr);
                    parent->slotkey[parentslot] = found_result.lastkey;
                } else {
                    myres |= result_t(btree_update_lastkey, found_result.lastkey);
                }
            }

            if (found_result.has(btree_fixmerge)) {
                // either the current node or the next is empty and should be
                // removed
                if (inner->childid[slot]->slotuse != 0)
                    slot++;

                // this is the child slot invalidated by the merge
                REPLAY_TREE_ASSERT(inner->childid[slot]->slotuse == 0);

                free_node(inner->childid[slot]);

                std::copy(inner->slotkey + slot, inner->slotkey + inner->slotuse, inner->slotkey + slot - 1);
                std::copy(inner->childid + slot + 1, inner->childid + inner->slotuse + 1, inner->childid + slot);

                inner->slotuse--;
                update_subtree_size(inner);

                if (inner->level == 1) {
                    // fix split key for children leaves
                    slot--;
                    auto child = static_cast<LeafNode*>(inner->childid[slot]);
                    inner->slotkey[slot] = child->key(child->slotuse - 1);
                }
            }

            if (inner->is_underflow() && !(inner == root_ && inner->slotuse >= 1)) {
                // case: the inner node is the root and has just one
                // child. that child becomes the new root
                if (left_inner == nullptr && right_inner == nullptr) {
                    REPLAY_TREE_ASSERT(inner == root_);
                    REPLAY_TREE_ASSERT(inner->slotuse == 0);

                    add_delay(inner->childid[0], inner->delay);
                    inner->delay = 0;
                    root_ = inner->childid[0];
                    inner->slotuse = 0;
                    free_node(inner);

                    return {btree_ok, found_delay};
                }
                // case : if both left and right leaves would underflow in case
                // of a shift, then merging is necessary. choose the more local
                // merger with our parent
                else if ((left_inner == nullptr || left_inner->is_few()) &&
                         (right_inner == nullptr || right_inner->is_few())) {
                    if (left_parent == parent) {
                        myres |= merge_inner(left_inner, inner, left_parent, parentslot - 1, left_delay, delay);
                    } else {
                        myres |= merge_inner(inner, right_inner, right_parent, parentslot, delay, right_delay);
                    }
                }
                // case : the right leaf has extra data, so balance right with
                // current
                else if ((left_inner != nullptr && left_inner->is_few()) &&
                         (right_inner != nullptr && !right_inner->is_few())) {
                    if (right_parent == parent) {
                        shift_left_inner(inner, right_inner, right_parent, parentslot, delay, right_delay);
                    } else {
                        myres |= merge_inner(left_inner, inner, left_parent, parentslot - 1, left_delay, delay);
                    }
                }
                // case : the left leaf has extra data, so balance left with
                // current
                else if ((left_inner != nullptr && !left_inner->is_few()) &&
                         (right_inner != nullptr && right_inner->is_few())) {
                    if (left_parent == parent) {
                        shift_right_inner(left_inner, inner, left_parent, parentslot - 1, left_delay, delay);
                    } else {
                        myres |= merge_inner(inner, right_inner, right_parent, parentslot, delay, right_delay);
                    }
                }
                // case : both the leaf and right leaves have extra data and our
                // parent, choose the leaf with more data
                else if (left_parent == right_parent) {
                    if (left_inner->slotuse <= right_inner->slotuse) {
                        shift_left_inner(inner, right_inner, right_parent, parentslot, delay, right_delay);
                    } else {
                        shift_right_inner(left_inner, inner, left_parent, parentslot - 1, left_delay, delay);
                    }
                } else {
                    if (left_parent == parent) {
                        shift_right_inner(left_inner, inner, left_parent, parentslot - 1, left_delay, delay);
                    } else {
                        shift_left_inner(inner, right_inner, right_parent, parentslot, delay, right_delay);
                    }
                }
            }

            return {myres, found_delay};
        }
    }

    //! Merge two leaf nodes. The function moves all key/data pairs from right
    //! to left and sets right's slotuse to zero. The right slot is then removed
    //! by the calling parent node.
    result_t merge_leaves(LeafNode* left, LeafNode* right, InnerNode* parent, std::int64_t left_delay,
                          std::int64_t right_delay) {
        (void)parent;

        REPLAY_TREE_ASSERT(left->is_leafnode() && right->is_leafnode());
        REPLAY_TREE_ASSERT(parent->level == 1);

        REPLAY_TREE_ASSERT(left->slotuse + right->slotuse < leaf_slotmax);

        for (std::size_t i = 0; i < right->slotuse; ++i) {
            right->delays[i] += (right_delay - left_delay);
        }

        std::copy(right->slotdata, right->slotdata + right->slotuse, left->slotdata + left->slotuse);
        std::copy(right->delays, right->delays + right->slotuse, left->delays + left->slotuse);

        left->slotuse += right->slotuse;

        left->next_leaf = right->next_leaf;
        if (left->next_leaf)
            left->next_leaf->prev_leaf = left;
        else
            tail_leaf_ = left;

        right->slotuse = 0;

        return btree_fixmerge;
    }

    //! Merge two inner nodes. The function moves all key/childid pairs from
    //! right to left and sets right's slotuse to zero. The right slot is then
    //! removed by the calling parent node.
    static result_t merge_inner(InnerNode* left, InnerNode* right, InnerNode* parent, unsigned int parentslot,
                                std::int64_t left_delay, std::int64_t right_delay) {
        REPLAY_TREE_ASSERT(left->level == right->level);
        REPLAY_TREE_ASSERT(parent->level == left->level + 1);

        REPLAY_TREE_ASSERT(parent->childid[parentslot] == left);

        REPLAY_TREE_ASSERT(left->slotuse + right->slotuse < inner_slotmax);

        if (self_verify) {
            // find the left node's slot in the parent's children
            unsigned int leftslot = 0;
            while (leftslot <= parent->slotuse && parent->childid[leftslot] != left)
                ++leftslot;

            REPLAY_TREE_ASSERT(leftslot < parent->slotuse);
            REPLAY_TREE_ASSERT(parent->childid[leftslot] == left);
            REPLAY_TREE_ASSERT(parent->childid[leftslot + 1] == right);

            REPLAY_TREE_ASSERT(parentslot == leftslot);
        }

        // retrieve the decision key from parent
        left->slotkey[left->slotuse] = parent->slotkey[parentslot];
        left->slotuse++;

        std::int64_t delay_diff = (right->delay + right_delay) - (left->delay + left_delay);
        for (std::size_t i = 0; i <= right->slotuse; ++i) {
            add_delay(right->childid[i], delay_diff);
        }

        // copy over keys and children from right
        std::copy(right->slotkey, right->slotkey + right->slotuse, left->slotkey + left->slotuse);
        std::copy(right->childid, right->childid + right->slotuse + 1, left->childid + left->slotuse);

        left->slotuse += right->slotuse;
        update_subtree_size(left);
        right->slotuse = 0;
        right->subtree_size = 0;

        return btree_fixmerge;
    }

    //! Balance two leaf nodes. The function moves key/data pairs from right to
    //! left so that both nodes are equally filled. The parent node is updated
    //! if possible.
    static result_t shift_left_leaf(LeafNode* left, LeafNode* right, InnerNode* parent, unsigned int parentslot,
                                    std::int64_t left_delay, std::int64_t right_delay) {
        REPLAY_TREE_ASSERT(left->is_leafnode() && right->is_leafnode());
        REPLAY_TREE_ASSERT(parent->level == 1);

        REPLAY_TREE_ASSERT(left->next_leaf == right);
        REPLAY_TREE_ASSERT(left == right->prev_leaf);

        REPLAY_TREE_ASSERT(left->slotuse < right->slotuse);
        REPLAY_TREE_ASSERT(parent->childid[parentslot] == left);

        auto shiftnum = static_cast<unsigned short>((right->slotuse - left->slotuse) >> 1u);

        REPLAY_TREE_ASSERT(left->slotuse + shiftnum < leaf_slotmax);

        // copy the first items from the right node to the last slot in the left
        // node.

        for (std::size_t i = 0; i < shiftnum; ++i) {
            right->delays[i] += (right_delay - left_delay);
        }

        std::copy(right->slotdata, right->slotdata + shiftnum, left->slotdata + left->slotuse);
        std::copy(right->delays, right->delays + shiftnum, left->delays + left->slotuse);

        left->slotuse += shiftnum;

        // shift all slots in the right node to the left

        std::copy(right->slotdata + shiftnum, right->slotdata + right->slotuse, right->slotdata);
        std::copy(right->delays + shiftnum, right->delays + right->slotuse, right->delays);

        right->slotuse -= shiftnum;

        // fixup parent
        if (parentslot < parent->slotuse) {
            parent->slotkey[parentslot] = left->key(left->slotuse - 1);
            return btree_ok;
        } else {  // the update is further up the tree
            return result_t(btree_update_lastkey, left->key(left->slotuse - 1));
        }
    }

    //! Balance two inner nodes. The function moves key/data pairs from right to
    //! left so that both nodes are equally filled. The parent node is updated
    //! if possible.
    static void shift_left_inner(InnerNode* left, InnerNode* right, InnerNode* parent, unsigned int parentslot,
                                 std::int64_t left_delay, std::int64_t right_delay) {
        REPLAY_TREE_ASSERT(left->level == right->level);
        REPLAY_TREE_ASSERT(parent->level == left->level + 1);

        REPLAY_TREE_ASSERT(left->slotuse < right->slotuse);
        REPLAY_TREE_ASSERT(parent->childid[parentslot] == left);

        auto shiftnum = static_cast<unsigned short>((right->slotuse - left->slotuse) >> 1u);

        REPLAY_TREE_ASSERT(left->slotuse + shiftnum < inner_slotmax);

        if (self_verify) {
            // find the left node's slot in the parent's children and compare to
            // parentslot

            unsigned int leftslot = 0;
            while (leftslot <= parent->slotuse && parent->childid[leftslot] != left)
                ++leftslot;

            REPLAY_TREE_ASSERT(leftslot < parent->slotuse);
            REPLAY_TREE_ASSERT(parent->childid[leftslot] == left);
            REPLAY_TREE_ASSERT(parent->childid[leftslot + 1] == right);

            REPLAY_TREE_ASSERT(leftslot == parentslot);
        }

        // copy the parent's decision slotkey and childid to the first new key
        // on the left
        left->slotkey[left->slotuse] = parent->slotkey[parentslot];
        left->slotuse++;

        std::int64_t delay_diff = (right->delay + right_delay) - (left->delay + left_delay);
        for (std::size_t i = 0; i < shiftnum; ++i) {
            add_delay(right->childid[i], delay_diff);
        }

        // copy the other items from the right node to the last slots in the
        // left node.
        std::copy(right->slotkey, right->slotkey + shiftnum - 1, left->slotkey + left->slotuse);
        std::copy(right->childid, right->childid + shiftnum, left->childid + left->slotuse);

        left->slotuse += static_cast<unsigned short>(shiftnum - 1u);

        // fixup parent
        parent->slotkey[parentslot] = right->slotkey[shiftnum - 1];

        // shift all slots in the right node
        std::copy(right->slotkey + shiftnum, right->slotkey + right->slotuse, right->slotkey);
        std::copy(right->childid + shiftnum, right->childid + right->slotuse + 1, right->childid);

        right->slotuse -= shiftnum;
        update_subtree_size(left);
        update_subtree_size(right);
    }

    //! Balance two leaf nodes. The function moves key/data pairs from left to
    //! right so that both nodes are equally filled. The parent node is updated
    //! if possible.
    static void shift_right_leaf(LeafNode* left, LeafNode* right, InnerNode* parent, unsigned int parentslot,
                                 std::int64_t left_delay, std::int64_t right_delay) {
        REPLAY_TREE_ASSERT(left->is_leafnode() && right->is_leafnode());
        REPLAY_TREE_ASSERT(parent->level == 1);

        REPLAY_TREE_ASSERT(left->next_leaf == right);
        REPLAY_TREE_ASSERT(left == right->prev_leaf);
        REPLAY_TREE_ASSERT(parent->childid[parentslot] == left);

        REPLAY_TREE_ASSERT(left->slotuse > right->slotuse);

        auto shiftnum = static_cast<unsigned short>((left->slotuse - right->slotuse) >> 1u);

        if (self_verify) {
            // find the left node's slot in the parent's children
            unsigned int leftslot = 0;
            while (leftslot <= parent->slotuse && parent->childid[leftslot] != left)
                ++leftslot;

            REPLAY_TREE_ASSERT(leftslot < parent->slotuse);
            REPLAY_TREE_ASSERT(parent->childid[leftslot] == left);
            REPLAY_TREE_ASSERT(parent->childid[leftslot + 1] == right);

            REPLAY_TREE_ASSERT(leftslot == parentslot);
        }

        // shift all slots in the right node

        REPLAY_TREE_ASSERT(right->slotuse + shiftnum < leaf_slotmax);

        std::copy_backward(right->slotdata, right->slotdata + right->slotuse,
                           right->slotdata + right->slotuse + shiftnum);
        std::copy_backward(right->delays, right->delays + right->slotuse, right->delays + right->slotuse + shiftnum);

        right->slotuse += shiftnum;

        // copy the last items from the left node to the first slot in the right
        // node.

        for (std::size_t i = left->slotuse - shiftnum; i < left->slotuse; ++i) {
            left->delays[i] += (left_delay - right_delay);
        }

        std::copy(left->slotdata + left->slotuse - shiftnum, left->slotdata + left->slotuse, right->slotdata);
        std::copy(left->delays + left->slotuse - shiftnum, left->delays + left->slotuse, right->delays);

        left->slotuse -= shiftnum;

        parent->slotkey[parentslot] = left->key(left->slotuse - 1);
    }

    //! Balance two inner nodes. The function moves key/data pairs from left to
    //! right so that both nodes are equally filled. The parent node is updated
    //! if possible.
    static void shift_right_inner(InnerNode* left, InnerNode* right, InnerNode* parent, unsigned int parentslot,
                                  std::int64_t left_delay, std::int64_t right_delay) {
        REPLAY_TREE_ASSERT(left->level == right->level);
        REPLAY_TREE_ASSERT(parent->level == left->level + 1);

        REPLAY_TREE_ASSERT(left->slotuse > right->slotuse);
        REPLAY_TREE_ASSERT(parent->childid[parentslot] == left);

        auto shiftnum = static_cast<unsigned short>((left->slotuse - right->slotuse) >> 1u);

        if (self_verify) {
            // find the left node's slot in the parent's children
            unsigned int leftslot = 0;
            while (leftslot <= parent->slotuse && parent->childid[leftslot] != left)
                ++leftslot;

            REPLAY_TREE_ASSERT(leftslot < parent->slotuse);
            REPLAY_TREE_ASSERT(parent->childid[leftslot] == left);
            REPLAY_TREE_ASSERT(parent->childid[leftslot + 1] == right);

            REPLAY_TREE_ASSERT(leftslot == parentslot);
        }

        // shift all slots in the right node

        REPLAY_TREE_ASSERT(right->slotuse + shiftnum < inner_slotmax);

        std::copy_backward(right->slotkey, right->slotkey + right->slotuse, right->slotkey + right->slotuse + shiftnum);
        std::copy_backward(right->childid, right->childid + right->slotuse + 1,
                           right->childid + right->slotuse + 1 + shiftnum);

        right->slotuse += shiftnum;

        // copy the parent's decision slotkey and childid to the last new key on
        // the right
        right->slotkey[shiftnum - 1] = parent->slotkey[parentslot];

        // copy the remaining last items from the left node to the first slot in
        // the right node.

        for (std::size_t i = left->slotuse - shiftnum + 1u; i <= left->slotuse; ++i) {
            add_delay(left->childid[i], (left->delay + left_delay) - (right->delay + right_delay));
        }

        std::copy(left->slotkey + left->slotuse - shiftnum + 1, left->slotkey + left->slotuse, right->slotkey);
        std::copy(left->childid + left->slotuse - shiftnum + 1, left->childid + left->slotuse + 1, right->childid);

        // copy the first to-be-removed key from the left node to the parent's
        // decision slot
        parent->slotkey[parentslot] = left->slotkey[left->slotuse - shiftnum];

        left->slotuse -= shiftnum;
        update_subtree_size(left);
        update_subtree_size(right);
    }

    //! \}

   public:
    //! \name Verification of Tree Invariants
    //! \{

    //! Run a thorough verification of all tree invariants. The program
    //! aborts via assert() if something is wrong.
    void verify() const {
        key_type minkey, maxkey;
        tree_stats vstats;

        if (root_) {
            verify_node(root_, &minkey, &maxkey, vstats);

            /* assert(vstats.size == stats_.size); */
            /* assert(vstats.leaves == stats_.leaves); */
            /* assert(vstats.inner_nodes == stats_.inner_nodes); */
            assert(vstats.size == stats_.size);
            assert(vstats.leaves == stats_.leaves);
            assert(vstats.inner_nodes == stats_.inner_nodes);

            verify_leaflinks();
        }
    }

   private:
    //! Recursively descend down the tree and verify each node
    void verify_node(const node* n, key_type* minkey, key_type* maxkey, tree_stats& vstats) const {
        if (n->is_leafnode()) {
            const LeafNode* leaf = static_cast<const LeafNode*>(n);

            assert(leaf == root_ || !leaf->is_underflow());
            assert(leaf->slotuse > 0);

            for (unsigned short slot = 0; slot < leaf->slotuse - 1; ++slot) {
                assert(key_lessequal(leaf->key(slot), leaf->key(slot + 1)));
            }

            *minkey = leaf->key(0);
            *maxkey = leaf->key(leaf->slotuse - 1);

            vstats.leaves++;
            vstats.size += leaf->slotuse;
        } else  // !n->is_leafnode()
        {
            auto inner = static_cast<InnerNode const*>(n);
            vstats.inner_nodes++;

            assert(inner == root_ || !inner->is_underflow());
            assert(inner->slotuse > 0);

            for (unsigned short slot = 0; slot < inner->slotuse - 1; ++slot) {
                assert(key_lessequal(inner->key(slot), inner->key(slot + 1)));
            }

            for (unsigned short slot = 0; slot <= inner->slotuse; ++slot) {
                const node* subnode = inner->childid[slot];
                key_type subminkey = key_type();
                key_type submaxkey = key_type();

                assert(subnode->level + 1 == inner->level);
                verify_node(subnode, &subminkey, &submaxkey, vstats);

                if (slot == 0)
                    *minkey = subminkey;
                else
                    assert(key_greaterequal(subminkey, inner->key(slot - 1)));

                if (slot == inner->slotuse)
                    *maxkey = submaxkey;
                else
                    assert(key_equal(inner->key(slot), submaxkey));

                if (inner->level == 1 && slot < inner->slotuse) {
                    // children are leaves and must be linked together in the
                    // correct order
                    [[maybe_unused]] const LeafNode* leafa = static_cast<const LeafNode*>(inner->childid[slot]);
                    [[maybe_unused]] const LeafNode* leafb = static_cast<const LeafNode*>(inner->childid[slot + 1]);

                    assert(leafa->next_leaf == leafb);
                    assert(leafa == leafb->prev_leaf);
                }
                if (inner->level == 2 && slot < inner->slotuse) {
                    // verify leaf links between the adjacent inner nodes
                    const InnerNode* parenta = static_cast<const InnerNode*>(inner->childid[slot]);
                    const InnerNode* parentb = static_cast<const InnerNode*>(inner->childid[slot + 1]);

                    [[maybe_unused]] const LeafNode* leafa =
                        static_cast<const LeafNode*>(parenta->childid[parenta->slotuse]);
                    [[maybe_unused]] const LeafNode* leafb = static_cast<const LeafNode*>(parentb->childid[0]);

                    assert(leafa->next_leaf == leafb);
                    assert(leafa == leafb->prev_leaf);
                }
            }
        }
    }

    //! Verify the double linked list of leaves.
    void verify_leaflinks() const {
        const LeafNode* n = head_leaf_;

        assert(n->level == 0);
        assert(!n || n->prev_leaf == nullptr);

        unsigned int testcount = 0;

        while (n) {
            assert(n->level == 0);
            assert(n->slotuse > 0);

            for (unsigned short slot = 0; slot < n->slotuse - 1; ++slot) {
                assert(key_lessequal(n->key(slot), n->key(slot + 1)));
            }

            testcount += n->slotuse;

            if (n->next_leaf) {
                assert(key_lessequal(n->key(n->slotuse - 1), n->next_leaf->key(0)));

                assert(n == n->next_leaf->prev_leaf);
            } else {
                assert(tail_leaf_ == n);
            }

            n = n->next_leaf;
        }

        assert(testcount == size());
    }

    //! \}
};

//! STL-like iterator object for tree items. The iterator points to a
//! specific slot number in a leaf.
template <typename Node, bool IsConst>
class TreeIterator {
   public:
    // *** Types

    //! The key type of the btree. Returned by key().
    using key_type = typename Node::key_type;

    //! The value type of the btree. Returned by operator*().
    using value_type = std::conditional_t<IsConst, typename Node::value_type const, typename Node::value_type>;

    //! Reference to the value_type. STL required.
    using reference = value_type&;

    //! Pointer to the value_type. STL required.
    using pointer = value_type*;

    //! STL-magic iterator category
    using iterator_category = std::bidirectional_iterator_tag;

    //! STL-magic
    using difference_type = std::ptrdiff_t;

    //! Our own type
    using self_type = TreeIterator;

   private:
    // *** Members

    //! The currently referenced leaf node of the tree
    Node* curr_leaf;

    //! Current key/data slot referenced
    unsigned short curr_slot;

    template <typename, typename, typename, typename, typename, typename>
    friend class ReplayTree;

   public:
    // *** Methods

    //! Default-Constructor of a mutable iterator
    TreeIterator() : curr_leaf(nullptr), curr_slot(0) {
    }

    //! Initializing-Constructor of a mutable iterator
    TreeIterator(Node* l, unsigned short s) : curr_leaf(l), curr_slot(s) {
    }

    //! Copy-constructor from a reverse iterator
    TreeIterator(std::reverse_iterator<TreeIterator> const& it)  // NOLINT
        : curr_leaf(it.curr_leaf), curr_slot(it.curr_slot) {
    }

    //! Dereference the iterator.
    inline reference operator*() const {
        return curr_leaf->slotdata[curr_slot];
    }

    //! Dereference the iterator.
    inline pointer operator->() const {
        return &curr_leaf->slotdata[curr_slot];
    }

    //! Key of the current slot.
    inline key_type const& key() const {
        return curr_leaf->key(curr_slot);
    }

    //! Prefix++ advance the iterator to the next slot.
    TreeIterator& operator++() {
        if (curr_slot + 1u < curr_leaf->slotuse) {
            ++curr_slot;
        } else if (curr_leaf->next_leaf != nullptr) {
            curr_leaf = curr_leaf->next_leaf;
            curr_slot = 0;
        } else {
            // this is end()
            curr_slot = curr_leaf->slotuse;
        }
        return *this;
    }

    //! Postfix++ advance the iterator to the next slot.
    TreeIterator operator++(int) {
        auto tmp = *this;  // copy ourselves
        ++(*this);
        return tmp;
    }

    //! Prefix-- backstep the iterator to the last slot.
    TreeIterator& operator--() {
        if (curr_slot > 0) {
            --curr_slot;
        } else if (curr_leaf->prev_leaf != nullptr) {
            curr_leaf = curr_leaf->prev_leaf;
            curr_slot = curr_leaf->slotuse - 1;
        } else {
            // this is begin()
            curr_slot = 0;
        }
        return *this;
    }

    //! Postfix-- backstep the iterator to the last slot.
    TreeIterator operator--(int) {
        auto tmp = *this;  // copy ourselves
        --(*this);
        return tmp;
    }

    //! Equality of iterators.
    inline bool operator==(TreeIterator const& x) const noexcept {
        return (x.curr_leaf == curr_leaf) && (x.curr_slot == curr_slot);
    }

    //! Inequality of iterators.
    inline bool operator!=(TreeIterator const& x) const noexcept {
        return (x.curr_leaf != curr_leaf) || (x.curr_slot != curr_slot);
    }
};

//! \}
//! \}

#endif  //! REPLAY_TREE_HPP_INCLUDED
