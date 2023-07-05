#import numpy as np
#import pandas as pd
from project_art import logo3_1
import graphs_main


def page_one():
    print("You have following options: ")
    print("-) Graph of employment types:           1")
    print("-) Graph of married unmarried approval: 2")
    print("-) Graph of highest transaction number: 3")
    print("-) Graph of highest transaction branch: 4")
    functions = {'1':visual_1, '2':visual_2, '3':visual_3, '4':visual_4}
    cancel = 3
    while cancel > 0:
        selection = input("Select a choice")
        if selection not in "1234":
            cancel -= 1
            selection = input("Please enter a valid value (1,2,3,4):")
            print(f"You can try {cancel} more times")
        else:
            functions[selection](df_loan_app)
