num1 = input('Enter a number: ')
num2 = input('Enter another number: ')


try:
    if not isinstance(num1, (int, float)):
        num1 = float(num1)

    if not isinstance(num2, (int, float)):
        num2 = float(num2)

    print(num1/num2)
except ZeroDivisionError:
    print("You can't divide by zero!")
except ValueError:
    print("Please enter valid numbers.")
except Exception as e:
    print(f"An error occurred: {e}")