import pandas as pd
from general_cleaning import LAPASV

def additionalCleaning(df, projectid, QuestionCodeDict):
    #1
    PrevQuestionCode = 'Q0000029'
    NextQuestionCode = 'Q0000030'
    df = LAPASV(df, projectid, PrevQuestionCode, NextQuestionCode, QuestionCodeDict)

    #2
    PrevQuestionCode = 'Q0000029'
    NextQuestionCode = 'Q0000025'
    df = LAPASV(df, projectid, PrevQuestionCode, NextQuestionCode, QuestionCodeDict)

    return df