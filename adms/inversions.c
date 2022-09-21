#include <assert.h>
#include "inversions.h"

int enable_inv_count_ = 0;

void enable_inv_count() {
    enable_inv_count_ = 1;
}

size_t max(size_t a, size_t b) { return a < b ? b : a; }

// An AVL tree node
struct Node
{
    int key;
    size_t height;
    struct Node *left, *right;
    size_t size; // size of the tree rooted with this Node
};

// A utility function to get height of the tree rooted with N
size_t height(struct Node *N)
{
    if (N == NULL)
        return 0;
    return N->height;
}

// A utility function to size of the tree of rooted with N
size_t size(struct Node *N)
{
    if (N == NULL)
        return 0;
    return N->size;
}

/* Helper function that allocates a new Node with the given key and
    NULL left and right pointers. */
struct Node* newNode(int key)
{
    struct Node* node = (struct Node*)malloc(sizeof(struct Node));
    node->key    = key;
    node->left   = node->right = NULL;
    node->height = node->size  = 1;
    return(node);
}

// A utility function to right rotate subtree rooted with y
struct Node *rightRotate(struct Node *y)
{
    struct Node *x = y->left;
    struct Node *T2 = x->right;

    // Perform rotation
    x->right = y;
    y->left = T2;

    // Update heights
    y->height = max(height(y->left), height(y->right))+1;
    x->height = max(height(x->left), height(x->right))+1;

    // Update sizes
    y->size = size(y->left) + size(y->right) + 1;
    x->size = size(x->left) + size(x->right) + 1;

    // Return new root
    return x;
}

// A utility function to left rotate subtree rooted with x
struct Node *leftRotate(struct Node *x)
{
    struct Node *y = x->right;
    struct Node *T2 = y->left;

    // Perform rotation
    y->left = x;
    x->right = T2;

    //  Update heights
    x->height = max(height(x->left), height(x->right)) + 1;
    y->height = max(height(y->left), height(y->right)) + 1;

    // Update sizes
    x->size = size(x->left) + size(x->right) + 1;
    y->size = size(y->left) + size(y->right) + 1;

    // Return new root
    return y;
}

// Get Balance factor of Node N
long long getBalance(struct Node *N)
{
    if (N == NULL)
        return 0;

    assert(height(N->left) < ((size_t) - 1) / 2);
    assert(height(N->right) < ((size_t) - 1) / 2);
    return (long long)height(N->left) - (long long)height(N->right);
}

// Inserts a new key to the tree rotted with Node. Also, updates
// *result (inversion count)
struct Node* insert(struct Node* node, int key, size_t *result)
{
    /* 1.  Perform the normal BST rotation */
    if (node == NULL)
        return(newNode(key));

    if (key < node->key)
    {
        node->left  = insert(node->left, key, result);

        // UPDATE COUNT OF GREATER ELEMENTS FOR KEY
        *result = *result + size(node->right) + 1;
    }
    else
        node->right = insert(node->right, key, result);

    /* 2. Update height and size of this ancestor node */
    node->height = max(height(node->left),
                       height(node->right)) + 1;
    node->size = size(node->left) + size(node->right) + 1;

    /* 3. Get the balance factor of this ancestor node to
          check whether this node became unbalanced */
    long long balance = getBalance(node);

    // If this node becomes unbalanced, then there are
    // 4 cases

    // Left Left Case
    if (balance > 1 && key < node->left->key)
        return rightRotate(node);

    // Right Right Case
    if (balance < -1 && key > node->right->key)
        return leftRotate(node);

    // Left Right Case
    if (balance > 1 && key > node->left->key)
    {
        node->left =  leftRotate(node->left);
        return rightRotate(node);
    }

    // Right Left Case
    if (balance < -1 && key < node->right->key)
    {
        node->right = rightRotate(node->right);
        return leftRotate(node);
    }

    /* return the (unchanged) node pointer */
    return node;
}

// The following function returns inversion count in arr[]
size_t getInvCount(const int arr[], size_t n)
{
    if (!enable_inv_count_)
        return 0;

    struct Node *root = NULL;  // Create empty AVL Tree

    size_t result = 0;   // Initialize result

    // Starting from first element, insert all elements one by
    // one in an AVL tree.
    for (size_t i = 0; i < n; i++)
        // Note that address of result is passed as insert
        // operation updates result by adding count of elements
        // greater than arr[i] on left of arr[i]
        root = insert(root, arr[i], &result);

    return result;
}


void print_array_structure(const array_t* array)
{
    printf("From %lu to %lu (%lu): ", array->offset, array->offset + array->size - 1, array->size);
    for(size_t i = array->offset; i < array->offset + array->size; i++)
        printf("%lu ", array->arr[i]);
    if (array->inversions != -1llu)
        printf(" (%lu inversions)", array->inversions);
    printf("\n");
}

array_t count_inversions(array_t array)
{
    array_t result = array;

    if (array.size == 0 || array.size == 1)
    {
        result.inversions = 0;
        return result;
    }

    array_t left = result;
    array_t right = result;
    left.size = array.size / 2;
    right.offset = left.offset + left.size;
    right.size = array.size - left.size;

    left = count_inversions(left);
    right = count_inversions(right);

    result.inversions = left.inversions + right.inversions;

    size_t merged[array.size];
    size_t i = 0;
    size_t j = 0;
    size_t k = 0;

    while (i < left.size && j < right.size) {
        if (left.arr[left.offset + i] <= right.arr[right.offset + j]) {
            merged[k++] = left.arr[left.offset + i];
            i++;
        }
        else {
            merged[k++] = right.arr[right.offset + j];
            j++;
            result.inversions += left.size - i;
        }
    }

    for(; i < left.size; i++)
        merged[k++] = left.arr[left.offset + i];
    for(; j < right.size; j++)
        merged[k++] = right.arr[right.offset + j];

    for(size_t l = 0; l < array.size; l++)
    {
        array.arr[array.offset + l] = merged[l];
    }

    return result;
}

size_t count_inversions_wrapper(const int* a, const size_t size) {
    if (!enable_inv_count_)
        return 0;

    int* buffer = (int*)malloc(size * sizeof(int));
    if (!buffer) {
        fprintf(stderr, "Couldn't allocate buffer for count_inversions\n");
        exit(5);
    }

    for(size_t i = 0; i < size; i++)
        buffer[i] = a[i];

    array_t arr;
    arr.inversions = 0;
    arr.arr = buffer;
    arr.size = size;
    arr.offset = 0;
    array_t output = count_inversions(arr);

    free(buffer);
    return output.inversions;
}
