from random import randrange, getrandbits


def print_children(depth):
    for num in range(0, randrange(1, 5)):
        if bool(getrandbits(1)) and depth < 10:
            print("ThreadTest::MessageWithReplies(vec![", end="")
            print_children(depth + 1)
            print("]),", end="")
        else:
            print("ThreadTest::Message,", end="")


print("ThreadTest::Root(vec![", end="")
print_children(0)
print("])")
