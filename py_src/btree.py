class Node:
    def __init__(self, value, left=None, right=None):
        self.value = value
        self.left = left
        self.right = right

    def append(self, value):
        if self.value > value:
            if self.left is None:
                self.left = Node(value)
            else:
                self.left.append(value)
        else:
            if self.right is None:
                self.right = Node(value)
            else:
                self.right.append(value)


def ordered_tree(node):
    if node is None:
        return

    ordered_tree(node.left)
    print(node.value)
    ordered_tree(node.right)


root = Node(10)

root.append(5)
root.append(8)
root.append(4)
root.append(12)

ordered_tree(root)
