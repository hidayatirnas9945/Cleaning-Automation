import pandas as pd
import numpy as np
import general_cleaning as gc
from sqlalchemy import create_engine
from additional_cleaning import additionalCleaning

def GetConnectionFromFile():
    file = open('Connection', 'r')
    conn = {}
    for each in file.readlines():
        conn[each.split(':')[0]] = each.split(':')[1].replace('\n', '')
    return conn

def GetQuestionFromDatabase(projectid):
    query = '''select pt.ProjectId PROJECT_ID, st.Counter SECTION_COUNTER, qt.Counter QUESTION_COUNTER, 
            st.code SECTION_CODE, qt.code QUESTION_CODE, qit.code ANSWER_CODE,
            prt_qtype.Code QUESTION_TYPE, prt_logic.Code QUESTION_LOGIC, st_next.Code NEXT_SECTION_CODE,
            qt_next.Code NEXT_QUESTION, prt_next_qtype.Code NEXT_QUESTION_TYPE
            from 
            questiontbl qt join sectiontbl st on qt.SectionId = st.SectionId and qt.QuestionStatusId = 2947
            join projecttbl pt on pt.ProjectId = st.ProjectId and st.ProjectId = %d
            left join questionitemtbl qit on qt.QuestionId = qit.QuestionId
            left join questionlogictbl qlt on qlt.QuestionItemId = qit.QuestionItemId
            left join parametertbl prt_logic on prt_logic.ParameterId = qlt.LogicTypeId
            left join questiontbl qt_next on qlt.NextQuestionId = qt_next.QuestionId
            left join parametertbl prt_qtype on qt.QuestionTypeId = prt_qtype.ParameterId
            left join parametertbl prt_next_qtype on qt_next.QuestionTypeId = prt_next_qtype.ParameterId
            left join sectiontbl st_next on st_next.SectionId = qt_next.SectionId
            order by SECTION_COUNTER, QUESTION_COUNTER;''' % (projectid)
    conn = GetConnectionFromFile()
    engine = create_engine('mysql+pymysql://%s:%s@%s:%s/%s' % (conn['username'], conn['password'], conn['host'], conn['port'], conn['database']))
    Question = pd.read_sql(query, engine)
    Question = Question.applymap(lambda x: gc.convertToIntOrString(x)).sort_values(by=['SECTION_COUNTER', 'QUESTION_COUNTER']).fillna('NONE')
    return Question

def GetQuestionList(Question):
    QuestionList = []
    tmpQuestionList = [tuple(x) for x in Question[['SECTION_CODE', 'QUESTION_CODE', 'QUESTION_TYPE']].values]
    for each in tmpQuestionList:
        if not each in QuestionList:
            QuestionList.append(each)
    return QuestionList

def CleaningData(df, projectid):
    Question = GetQuestionFromDatabase(projectid)
    QuestionGroup = Question.groupby(['SECTION_COUNTER', 'QUESTION_COUNTER', 'SECTION_CODE', 'QUESTION_TYPE', 'QUESTION_CODE', 'QUESTION_LOGIC','NEXT_SECTION_CODE', 'NEXT_QUESTION', 'NEXT_QUESTION_TYPE'])
    # QuestionGroup_CheckNextQuestion = Question[['QUESTION_CODE', 'QUESTION_LOGIC', 'ANSWER_CODE']][~Question['QUESTION_LOGIC'].isin(['TIPA', 'JQPA', 'SQPA'])].groupby('QUESTION_CODE')
    QuestionList = GetQuestionList(Question)

    df, dfColumns = gc.cleaningColumns(df)
    df = gc.cleaningRows(df)

    # cleaning data
    QuestionCodeDict = {}
    for sectionCode, questionCode, questionType in QuestionList:
        if not questionCode in QuestionCodeDict.keys():
            if questionType == 'RADIOBUTTON' and gc.isExistColumn_RadioButton(df, projectid, questionCode):
                QuestionCodeDict[questionCode] = questionCode
            elif questionType == 'CHECKBOX':
                exist, columns = gc.isExistColumn_CheckBox(df, projectid, questionCode)
                if exist:
                    QuestionCodeDict[questionCode] = columns

    QuestionDoneChecked = []
    for sectionCounter, questionCounter, sectionCode, questionType, questionCode, questionLogic, nextSectionCode, nextQuestion, nextQuestionType in QuestionGroup.groups:
        if questionCode in QuestionCodeDict.keys():
            if not questionCode in QuestionDoneChecked:
                if questionType == 'RADIOBUTTON':
                    answerList = \
                    Question[['QUESTION_CODE', 'ANSWER_CODE']].groupby('QUESTION_CODE').get_group(questionCode)[
                        'ANSWER_CODE'].unique().tolist() + [np.nan]
                    df = gc.checkRejectStatus_RadioButton(df, projectid, questionCode)
                    df = gc.checkValueRange(df, projectid, questionCode, answerList)
                    df = gc.checkSpecify_RadioButton(df, projectid, questionCode)
                elif questionType == 'CHECKBOX':
                    df = gc.checkRejectStatus_Checkbox(df, projectid, questionCode, QuestionCodeDict)
                    if not sectionCode in ['Z']:
                        df = gc.checkIndependentAnswer(df, projectid, questionCode, QuestionCodeDict, withLogic=False)
                    else:
                        df = gc.checkIndependentAnswer(df, projectid, questionCode, QuestionCodeDict)
                    df = gc.checkSpecify_Checkbox(df, projectid, questionCode, QuestionCodeDict)
                QuestionDoneChecked.append(questionCode)

            if questionLogic == 'TIPA':
                answerList = QuestionGroup.get_group((sectionCounter, questionCounter, sectionCode, questionType,
                                                      questionCode, questionLogic, nextSectionCode, nextQuestion,
                                                      nextQuestionType))['ANSWER_CODE'].unique().tolist()
                df = gc.TIPA(df, projectid, questionCode, answerList)
            elif questionLogic == 'JQPA':
                startIndex = QuestionList.index((sectionCode, questionCode, questionType)) + 1
                endIndex = QuestionList.index((nextSectionCode, nextQuestion, nextQuestionType))
                QuestionListToPassOver = QuestionList[startIndex:endIndex]
                answerList = QuestionGroup.get_group((sectionCounter, questionCounter, sectionCode, questionType,
                                                      questionCode, questionLogic, nextSectionCode, nextQuestion,
                                                      nextQuestionType))['ANSWER_CODE'].unique().tolist()
                if questionType == 'RADIOBUTTON':
                    df = gc.JQPA_RadioButton(df, projectid, questionCode, answerList, QuestionListToPassOver,
                                             QuestionCodeDict)
                elif questionType == 'CHECKBOX':
                    df = gc.JQPA_CheckBox(df, projectid, questionCode, answerList, QuestionListToPassOver,
                                          QuestionCodeDict)
            elif nextQuestion in QuestionCodeDict.keys():
                if questionLogic == 'SAPA':
                    if questionType == 'RADIOBUTTON':
                        answerList = sorted(Question[['QUESTION_CODE', 'ANSWER_CODE']].groupby('QUESTION_CODE').get_group(nextQuestion)['ANSWER_CODE'].unique().tolist())
                        df = gc.SAPA_RadioButton(df, projectid, questionCode, nextQuestion, answerList)
                    elif questionType == 'CHECKBOX':
                        df = gc.SAPA_CheckBox(df, projectid, questionCode, nextQuestion, nextQuestionType, QuestionCodeDict)
                elif questionLogic == 'HAPA':
                    if questionType == 'RADIOBUTTON':
                        df = gc.HAPA_RadioButton(df, projectid, questionCode, nextQuestion, nextQuestionType, QuestionCodeDict)
                    elif questionType == 'CHECKBOX':
                        df = gc.HAPA_CheckBox(df, projectid, questionCode, nextQuestion, nextQuestionType, QuestionCodeDict)
                elif questionLogic == 'SQPA':
                    answerList = QuestionGroup.get_group((sectionCounter, questionCounter, sectionCode, questionType,
                                                          questionCode, questionLogic, nextSectionCode, nextQuestion,
                                                          nextQuestionType))['ANSWER_CODE'].unique().tolist()
                    if questionType == 'RADIOBUTTON':
                        df = gc.SQPA_RadioButton(df, projectid, questionCode, answerList, nextQuestion,
                                                 nextQuestionType, QuestionCodeDict)
                    elif questionType == 'CHECKBOX':
                        df = gc.SQPA_CheckBox(df, projectid, questionCode, answerList, nextQuestion, nextQuestionType,
                                              QuestionCodeDict)
                elif questionLogic == 'HQPA':
                    answerList = QuestionGroup.get_group((sectionCounter, questionCounter, sectionCode, questionType,
                                                          questionCode, questionLogic, nextSectionCode, nextQuestion,
                                                          nextQuestionType))['ANSWER_CODE'].unique().tolist()
                    if questionType == 'RADIOBUTTON':
                        df = gc.HQPA_RadioButton(df, projectid, questionCode, answerList, nextQuestion,
                                                 nextQuestionType, QuestionCodeDict)
                    elif questionType == 'CHECKBOX':
                        df = gc.HQPA_CheckBox(df, projectid, questionCode, answerList, nextQuestion, nextQuestionType,
                                              QuestionCodeDict)
    df = additionalCleaning(df, projectid, QuestionCodeDict)
    df = gc.summarizeRemarks(df)
    df = gc.returnInitialColumns(df, dfColumns)
    return df, Question

if __name__ == '__main__':
    projectid = 420
    file_name = 'pr420.xlsx'
    if file_name.endswith('.csv'):
        df = pd.read_csv(file_name, low_memory=False)
    elif file_name.endswith('.xlsx'):
        df = pd.read_excel(file_name)
    df, Question = CleaningData(df, projectid)
    # Question = GetQuestionFromDatabase(projectid)
