import time


def cache(func):
    cache_dict = {}

    def wrapper(*args):
        if args not in cache_dict:
            cache_dict[args] = func(*args)
            print(cache_dict, "\n")
        return cache_dict[args]

    return wrapper


@cache
def fib(n):
    if n < 2:
        return n
    return fib(n - 1) + fib(n - 2)


def fibonacci(n):
    a, b = 0, 1
    for _ in range(n):
        a, b = b, a + b
    return a


@cache
def func1(n):
    return n**1_000_000


def func2(*nums):
    for num in nums:
        print(type(func1(num)))


start = time.perf_counter()
# print(fib(39))
print(fibonacci(40))
# print(func2(100,200,150,200,100))
print(f"Process finished in {time.perf_counter() - start:.1f} seconds")
