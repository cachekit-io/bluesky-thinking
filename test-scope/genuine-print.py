# Python file — this SHOULD trigger "Replace print statements with logging framework"
import os


def process_data(items):
    for item in items:
        print(f"Processing: {item}")
        result = item.upper()
        print(f"Result: {result}")
    print("All done!")
