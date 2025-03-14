

def printout(func):
    def wrapper(*arg):
        print(f'doing something for {arg}')
        return func(*arg)
    return wrapper

def check_errors(func):
    def wrapper(*args):
        try:
            return func(*args)
        except Exception as e:
            return f'function {func.__name__} returned an error: {e}'
    return wrapper

@check_errors
def func1(a):
    return a*a

@check_errors
def func2(a):
    return a*a

@check_errors
def func3(a):
    return a*a

@check_errors
def func4(a):
    return a*a

print(func1('200'))
print(func2(200))
print(func3())
print(func4(2,4,6))