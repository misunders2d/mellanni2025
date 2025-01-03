import requests

class Box:
    def __init__(self, kind, max_qty):
        self.kind = kind
        self.current_qty = 0
        self.max_qty = max_qty
        self.content = []
        
    def add_products(self, products:list):
        for product in products:
            if self.current_qty+product.qty < self.max_qty:
                self.current_qty+=product.qty
                self.content.append(product)
            else:
                print(f'Sorry, limit of {self.max_qty} will be exceeded')
        print(self.current_qty)

    def __repr__(self): # represenation of object in console
        return f'Box with {self.kind}, contains {len(self.content)} products with quantity {self.current_qty}\nMax qty: {self.max_qty}'

class Fruits:
    def __init__(self, kind, qty): #constructor method, which takes `self` and additional parameters
        self.kind = kind
        self.qty = qty

    def __str__(self): # string representation for printing
        return f'{self.kind} of quantity: {self.qty}'

    def __repr__(self): # represenation of object in console
        return f'Object of class {self.__class__}, kind = {self.kind} of quantity: {self.qty}'

    def __add__(self, other): # replaces `+` operator, defines behaviour
        if self.kind == other.kind:
            return Fruits(self.kind, self.qty + other.qty)
        else:
            return "Wrong kinds being added"

    def __ge__(self, other): # replaces `>+` operator, defines behaviour
        return self.qty <= other.qty

    def __gt__(self, other): # replaces `>` operator, defines behaviour
        return self.qty < other.qty
    
    def __eq__(self, other):# replaces `==` operator, defines behaviour
        return self.qty == other.qty

    def google(self):
        result = requests.get(f'https://www.google.com/search?q={self.kind}')
        return result.text
        
    


a = Fruits('apple', 2)
b = Fruits('banana', 5)
c = Fruits('apple', 8)
d = Fruits('apple',2)
products = [a,c, d]



box = Box('apple', 40)
box.add_products([a,c,d])


new_fruit = a + c
a - b
print(a >= c)
print(a == d)
