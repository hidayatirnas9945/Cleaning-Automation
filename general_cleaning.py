import numpy as np
import pandas as pd
import itertools

def isExistColumn_RadioButton(df, projectid, questionCode):
    if questionCode in df.columns:
        return True
    else:
        print('Project %d Column Error: there\'s no column named %s' % (projectid, questionCode))
        return False

def isExistColumn_CheckBox(df, projectid, questionCode):
    columns = [x for x in df.columns if questionCode+'_' in x and not 'REMARKS' in x and not 'SPECIFY' in x]
    if len(columns) > 0:
        return True, columns
    else:
        print('Project %d Column Error: there\'s no column named %s' % (projectid, questionCode))
        return False, columns

def isOneColumnNotNullAndOne(x, columns):
    tmp = False
    i = 0
    while(tmp != True and i < len(columns)):
        if pd.notnull(x[columns[i]]) and x[columns[i]] == 1:
            tmp = True
        i += 1
    return tmp

def isOneColumnNotNullAndZero(x, columns):
    tmp = False
    i = 0
    while(tmp != True and i < len(columns)):
        if pd.notnull(x[columns[i]] and x[columns[i]] == 0):
            tmp = True
        i += 1
    return tmp

def isOneColumnNotNull(x, columns):
    tmp = False
    i = 0
    while(tmp != True and i < len(columns)):
        if pd.notnull(x[columns[i]]):
            tmp = True
        i += 1
    return tmp

def isOneColumnNull(x, columns):
    tmp = False
    i = 0
    while(tmp!=True and i < len(columns)):
        if pd.isnull(x[columns[i]]):
            tmp = True
        i += 1
    return tmp

def createDeleteColumn(df, questionCode):
    if not questionCode + '_DELETE' in df.columns:
        df[questionCode + '_DELETE'] = np.nan
    return df

def createRemarksColumn(df, questionCode):
    if not questionCode + '_REMARKS' in df.columns:
        df[questionCode + '_REMARKS'] = np.nan
    return df

def createRejectColumn(df):
    if not 'R_STATUS' in df.columns:
        df['R_STATUS'] = np.nan
    if not 'REJECT_REMARKS' in df.columns:
        df['REJECT_REMARKS'] = np.nan
    return df

def convertToIntOrString(x):
    try:
        if pd.isnull(x):
            x = np.nan
        else:
            x = int(x)
    except:
        if isinstance(x, str):
            x = x.upper().strip()
    return x

def cleaningColumns(df):
    columns = df.columns
    df.columns = df.columns.str.strip().str.upper()
    return df, columns

def cleaningRows(df):
    df = df.applymap(lambda x: convertToIntOrString(x))
    df['GUID'] = df['GUID'].str.lower()
    if 'BIRTHDATE' in df.columns:
        df['BIRTHDATE'] = (pd.to_datetime(df['BIRTHDATE'])).dt.year
    if 'STARTDATE' in df.columns:
        df['STARTDATE'] = pd.to_datetime(df['STARTDATE'])
    if 'ENDDATE' in df.columns:
        df['ENDDATE'] = pd.to_datetime(df['ENDDATE'])
    return df

def summarizeRemarks(df):
    RemarksColumns = [x for x in df.columns if '_REMARKS' in x]
    DeleteColumns = [x for x in df.columns if '_DELETE' in x]
    df['EDIT_STATUS'] = df.apply(lambda x: checkEditStatus(x, RemarksColumns), axis = 1)
    df['DELETE_STATUS'] = df.apply(lambda x: checkDeleteStatus(x, DeleteColumns), axis = 1)
    df['REMARKS'] = df.apply(lambda x: 1 if x['EDIT_STATUS'] != 'None' or x['DELETE_STATUS'] != 'None' else np.nan, axis = 1)
    cols = RemarksColumns+DeleteColumns
    df.drop(columns = RemarksColumns+DeleteColumns, inplace = True)
    return df

def checkEditStatus(x, columns):
    result = ''
    for each in columns:
        if x[each] == 1:
            result += each.replace('_REMARKS', ';')
    if result == '':
        return 'None'
    else:
        return ((result[::-1]).replace(';', '', 1))[::-1]

def checkDeleteStatus(x, columns):
    result = ''
    for each in columns:
        if x[each] == 1:
            result += each.replace('_DELETE', ';')
    if result == '':
        return 'None'
    else:
        return ((result[::-1]).replace(';', '', 1))[::-1]

def returnInitialColumns(df, columns):
    columnsList = {x:y for x, y in zip(df.columns, columns) if x == y.upper()}
    df.rename(columns = columnsList, inplace=True)
    return df

def sumColumns(x, columns):
    Sum = 0
    for each in columns:
        if pd.notnull(x[each]) and isinstance(x[each], int):
            Sum += x[each]
        elif pd.notnull(x[each]) and isinstance(x[each], str):
            Sum += 1
    return Sum

def checkRejectStatus_RadioButton(df, projectid, questionCode):
    if 'R_STATUS' in df.columns:
        if (df[df['R_STATUS'] == 'REJECT'][questionCode].notnull()).any():
            print('Project %d Value Error (RJ): %s has already been rejected at index ' % (projectid, questionCode) +
                  str((df[(df['R_STATUS'] == 'REJECT') & (df[questionCode].notnull())].index).tolist()))
            df = createDeleteColumn(df, questionCode)
            df.loc[(df['R_STATUS'] == 'REJECT') & (df[questionCode].notnull()), questionCode + '_DELETE'] = 1
            df.loc[df['R_STATUS'] == 'REJECT', questionCode] = np.nan
    return df

def checkRejectStatus_Checkbox(df, projectid, questionCode, questionCodeDict):
    df['FLAG'] = df.apply(lambda x: isOneColumnNotNull(x, questionCodeDict[questionCode]), axis = 1)
    if 'R_STATUS' in df.columns:
        if (df[df['R_STATUS'] == 'REJECT']['FLAG'] == 1).any():
            print('Project %d Value Error (RJ): %s has already been rejected at index ' % (projectid, questionCode) +
                  str((df[(df['R_STATUS'] == 'REJECT') & (df['FLAG'] == 1)].index).tolist()))
            df = createRemarksColumn(df, questionCode)
            df.loc[(df['R_STATUS'] == 'REJECT') & (df['FLAG'] == 1), questionCode + '_REMARKS'] = 1
        for each in questionCodeDict[questionCode]:
            df.loc[df['R_STATUS'] == 'REJECT', each] = np.nan
    df.drop(columns = ['FLAG'], inplace = True)
    return df

def checkValueRange(df, projectid, questionCode, listAnswer):
    if (~df[questionCode].isin(listAnswer)).any():
        print('Project %d Value Error (VR): %s value not in range at index ' % (projectid, questionCode) +
              str((df[~df[questionCode].isin(listAnswer)].index).tolist()))
        df = createDeleteColumn(df, questionCode)
        df.loc[~df[questionCode].isin(listAnswer), questionCode + '_DELETE'] = 1
        df.loc[~df[questionCode].isin(listAnswer), questionCode] = np.nan
    return df

def checkSpecify_RadioButton(df, projectid, questionCode):
    if not questionCode in ['BD-DG-G-1']:
        if (df[questionCode] == 9996).any():
            if questionCode+ '_SPECIFY' in df.columns:
                if (df[df[questionCode] == 9996][questionCode + '_SPECIFY'].isnull()).any():
                    print('Project %d Value Error (SP): %s_SPECIFY is null while %s is 9996 at index ' %
                          (projectid, questionCode, questionCode)
                          + str((df[(df[questionCode] == 9996) & (df[questionCode + '_SPECIFY'].isnull())].index).tolist()))
                if (df[df[questionCode] != 9996][questionCode + '_SPECIFY'].notnull()).any():
                    print('Project %d Value Error (SP): %s_SPECIFY is not null while %s is not 9996 at index ' %
                          (projectid, questionCode, questionCode) +
                          str((df[(df[questionCode] != 9996) & (df[questionCode + '_SPECIFY'].notnull())].index).tolist()))
            else:
                print('Project %d Column Error (SP): there\'s no column named %s_SPECIFY while %s is 9996 at index ' %
                      (projectid, questionCode, questionCode) + str((df[df[questionCode] == 9996].index).tolist()))
    return df

def checkSpecify_Checkbox(df, projectid, questionCode, QuestionCodeDict):
    qc_spec = sorted([y for y in df.columns if (questionCode + '_' in y) and not('REMARKS' in y) and ('SPECIFY' in y)])
    columns = [(x, y) for x in QuestionCodeDict[questionCode] for y in qc_spec if x.split('_')[-1] == y.split('_')[-2]]
    for x, y in columns:
        if (df[df[x] == 1][y].isnull()).any():
            print('Project %d Value Error (SP): %s is null while %s is 1 at index ' % (projectid, y, x) +
                  str((df[(df[x] == 1) & (df[y].isnull())].index).tolist()))
        if (df[(df[x] == 0) | (pd.isnull(df[x]))][y].notnull()).any():
            print('Project %d Value Error (SP): %s is not null while %s is 0 or null at index ' % (projectid, y, x) +
                  str((df[((df[x] == 0) | (df[x].isnull())) & (df[y].notnull())].index).tolist()))
            df.loc[(df[x] == 0) | (df[x].isnull()), y] = np.nan
    return df

def checkIndependentAnswer(df, projectid, questionCode, QuestionCodeDict, withLogic = True):
    independentAnswerList = [questionCode + '_' + str(x) for x in [9998, 9999, 9997]
                             if questionCode + '_' + str(x) in QuestionCodeDict[questionCode]]
    questionCodeColumns = [x for x in QuestionCodeDict[questionCode] if not x in independentAnswerList]
    df['FLAG'] = df.apply(lambda x: isOneColumnNotNullAndOne(x, independentAnswerList) and isOneColumnNotNullAndOne(x, questionCodeColumns), axis = 1)
    if (df['FLAG'] == True).any():
        for x, y in itertools.combinations(independentAnswerList, 2):
            if (df[df[x] == 1][y] == 1).any():
                df = createRemarksColumn(df, questionCode)
                df.loc[(df[x] == 1) & (df[y] == 1), questionCode+'_REMARKS'] = 1
                df.loc[df[x] == 1, y] = 0
    if (df['FLAG'] == True).any():
        print('Project %d Value Error (IA): %s except [9997, 9998, 9999] is 1 '
              'while [9997, 9998, 9999] is 1 too at index ' % (projectid, questionCode) +
              str((df[df['FLAG'] == True].index).tolist()))
        df = createRemarksColumn(df, questionCode)
        df.loc[df['FLAG'] == True, questionCode + '_REMARKS'] = 1
        if withLogic == True:
            for each in questionCodeColumns:
                df.loc[df['FLAG'] == True, each] = 0
        else:
            for each in independentAnswerList:
                df.loc[df['FLAG'] == True, each] = 0
    df.drop(columns = ['FLAG'], inplace = True)
    return df

# def checkNextQuestionRadioButton(df, projectid, prevQC, conformingValues, nextQC, nextQC_type):
#     if prevQC in df.columns:
#         if nextQC_type == 'RADIOBUTTON':
#             if nextQC in df.columns:
#                 if (df[df[prevQC].isin(conformingValues)][nextQC].isnull()).any():
#                     print('Project %d Value Error: %s is null while respondent should answer it at index ' %
#                           (projectid, nextQC) + str((df[(df[prevQC].isin(conformingValues)) &
#                                                         (df[nextQC].isnull())].index).tolist()))
#             else:
#                 print('Project %d Column Error: there\'s no column named %s' % (projectid, nextQC))
#         elif nextQC_type == 'CHECKBOX':
#             nextCode_cols = [x for x in df.columns if (nextQC + '_' in x) and not ('REMARKS' in x) and not ('SPECIFY' in x)]
#             if len(nextCode_cols) > 0:
#                 df['FLAG'] = df.apply(lambda x: checkNextColumn_helper(x, nextCode_cols), axis = 1)
#                 if (df[df[prevQC].isin(conformingValues)]['FLAG'] == 0).any():
#                     print('Project %d Value Error: %s is null while respondent should answer it at index ' %
#                           (projectid, nextQC) + str((df[(df[prevQC].isin(conformingValues)) &
#                                                         (df['FLAG'] == 0)].index).tolist()))
#                     df.drop(columns = ['FLAG'], inplace = True)
#             else:
#                 print('Project %d Column Error: there\'s no column named %s' % (projectid, nextQC))
#     else:
#         print('Project %d Column Error: there\'s no column named %s' % (projectid, prevQC))
#     return df
#
# def checkNextQuestionCheckbox(df, projectid, prevQC, conformingValues, nextQC, nextQC_type):
#     columns = [prevQC + '_' + str(x) for x in conformingValues if (prevQC + '_' + str(x) in df.columns)]
#     if len(columns) > 0:
#         df['FLAG1'] = df.apply(lambda x: checkNextColumn_helper(x, columns), axis=1)
#         if nextQC_type == 'RADIOBUTTON':
#             if nextQC in df.columns:
#                 if (df[df['FLAG1'] == 1][nextQC].isnull()).any():
#                     print('Project %d Value Error: %s is null while respondent should answer it at index ' %
#                           (projectid, nextQC) + str((df[(df['FLAG1'] == 1) &
#                                                         (df[nextQC].isnull())].index).tolist()))
#             else:
#                 print('Project %d Column Error: there\'s no column named %s' % (projectid, nextQC))
#         elif nextQC_type == 'CHECKBOX':
#             nextCode_cols = [x for x in df.columns if (nextQC + '_' in x)]
#             if len(nextCode_cols) > 0:
#                 df['FLAG2'] = df.apply(lambda x: checkNextColumn_helper(x, nextCode_cols), axis = 1)
#                 if (df[df['FLAG1'] == 1]['FLAG2'] == 0).any():
#                     print('Project %d Value Error: %s is null while respondent should answer it at index ' %
#                           (projectid, nextQC) + str((df[(df['FLAG1'] == 1) &
#                                                         (df['FLAG2'] == 0)].index).tolist()))
#                 df.drop(columns = ['FLAG2'], inplace = True)
#             else:
#                 print('Project %d Column Error: there\'s no column named %s' % (projectid, nextQC))
#         df.drop(columns = ['FLAG1'], inplace = True)
#     return df
#
# def checkNextColumn_helper(x, columns):
#     tmp = 0
#     for each in columns:
#         if pd.notnull(x[each]) and (x[each] == 1):
#             tmp = 1
#     return tmp

def TIPA(df, projectid, questionCode, listAnswer):
    if (df[questionCode].isin(listAnswer)).any():
        print('Project %d Value Error (TIPA): %s is rejected at index ' % (projectid, questionCode) +
              str((df[df[questionCode].isin(listAnswer)].index).tolist()))
        df = createRejectColumn(df)
        df.loc[df[questionCode].isin(listAnswer), 'R_STATUS'] = 'REJECT'
        df.loc[df[questionCode].isin(listAnswer), 'REJECT_REMARK'] = questionCode
    return df

def JQPA_RadioButton(df, projectid, questionCode, conformingAnswer, questionListToPassOver, QuestionCodeDict):
    for sec, col, type in questionListToPassOver:
        if col in QuestionCodeDict.keys():
            if type == 'RADIOBUTTON':
                if (df[df[questionCode].isin(conformingAnswer)][col].notnull()).any():
                    print('Project %d Value Error (JQPA): respondent shouldn\'t answer %s at index ' % (projectid, col)
                          + str((df[(df[questionCode].isin(conformingAnswer)) & (df[col].notnull())].index).tolist()))
                    df = createDeleteColumn(df, col)
                    df.loc[(df[questionCode].isin(conformingAnswer) & (df[col].notnull())), col + '_DELETE'] = 1
                    df.loc[df[questionCode].isin(conformingAnswer), col] = np.nan
            if type == 'CHECKBOX':
                df['FLAG'] = df.apply(lambda x: isOneColumnNotNull(x, QuestionCodeDict[col]) and (x[questionCode] in conformingAnswer), axis = 1)
                if (df['FLAG'] == True).any():
                    print('Project %d Value Error (JQPA): respondent shouldn\'t answer %s at index ' % (projectid, col)
                          + str((df[df['FLAG'] == True].index).tolist()))
                    df = createRemarksColumn(df, col)
                    df.loc[df['FLAG'] == True, col + '_REMARKS'] = 1
                    for each in QuestionCodeDict[col]:
                        df.loc[df[questionCode].isin(conformingAnswer), each] = np.nan
                df.drop(columns = ['FLAG'], inplace = True)
    return df

def JQPA_CheckBox(df, projectid, questionCode, conformingAnswer, questionListToPassOver, QuestionCodeDict):
    questionCodeColumns = [questionCode +'_' + str(int(x)) for x in conformingAnswer
                           if questionCode +'_' + str(int(x)) in QuestionCodeDict[questionCode]]
    if len(questionCodeColumns) > 0:
        # df['FLAG1'] = df.apply(lambda x: isOneColumnNotNullAndOne(x, questionCodeColumns), axis = 1)
        for sec, col, type in questionListToPassOver:
            if type == 'RADIOBUTTON':
                if col in QuestionCodeDict.keys():
                    df['FLAG'] = df.apply(lambda x: isOneColumnNotNullAndOne(x, questionCodeColumns) and pd.notnull(x[col]), axis = 1)
                    # if (df[df['FLAG1'] == 1][col].notnull()).any():
                    if (df['FLAG'] == True).any():
                        print('Project %d Value Error (JQPA): respondent shouldn\'t answer %s at index ' %
                              (projectid, col) + str((df[df['FLAG'] == True].index).tolist()))
                        df = createDeleteColumn(df, col)
                        df.loc[df['FLAG'] == True, col+'_DELETE'] = 1
                        df.loc[df['FLAG'] == True, col] = np.nan
                    df.drop(columns = ['FLAG'], inplace = True)
            elif type == 'CHECKBOX':
                if col in QuestionCodeDict.keys():
                    df['FLAG'] = df.apply(lambda x: isOneColumnNotNullAndOne(x, questionCodeColumns) and isOneColumnNotNull(x, QuestionCodeDict[col]), axis = 1)
                    if (df['FLAG'] == True).any():
                        print('Project %d Value Error (JQPA): respondent shouldn\'t answer %s at index ' %
                              (projectid, col) + str((df[df['FLAG'] == True].index).tolist()))
                        df = createRemarksColumn(df, col)
                        df.loc[df['FLAG'] == True, col+'_REMARKS'] = 1
                        for each in QuestionCodeDict[col]:
                            df.loc[df['FLAG'] == True, each] = np.nan
                    df.drop(columns = ['FLAG'], inplace = True)
    return df

def SAPA_RadioButton(df, projectid, PrevQuestionCode, NextQuestionCode, ListAnswer):
    df['FLAG'] = df.apply(lambda x: SAPA_RadioButton_RadioButton(x, PrevQuestionCode, NextQuestionCode, ListAnswer), axis = 1)
    if (df['FLAG'] == False).any():
        print('Project %d Value Error (SAPA): the answer in %s is not based on answer in %s at index ' %
              (projectid, NextQuestionCode, PrevQuestionCode) + str((df[df['FLAG'] == False].index).tolist()))
        df = createDeleteColumn(df, NextQuestionCode)
        df.loc[df['FLAG'] == False, NextQuestionCode + '_DELETE'] = 1
        df.loc[df['FLAG'] == False, NextQuestionCode] = np.nan
    df.drop(columns = ['FLAG'], inplace = True)
    return df

def SAPA_RadioButton_RadioButton(x, PrevQuestionCode, NextQuestionCode, ListAnswer):
    # is the NextQuestionCode answer in the ListAnswer based on the PrevQuestionCode answer?
    if pd.isnull(x[PrevQuestionCode]) or pd.isnull(x[NextQuestionCode]) or x[NextQuestionCode] in (ListAnswer[:ListAnswer.index(x[PrevQuestionCode])+1]):
        tmp = True
    else:
        tmp = False
    return tmp

def SAPA_CheckBox(df, projectid, PrevQuestionCode, NextQuestionCode, NextQuestionCodeType, QuestionCodeDict):
    if NextQuestionCodeType == 'RADIOBUTTON':
        df['FLAG'] = df.apply(lambda x: SAPA_CheckBox_RadioButton(x, NextQuestionCode, QuestionCodeDict[PrevQuestionCode]), axis = 1)
        if (df['FLAG'] == False).any():
            print('Project %d Value Error (SAPA): the answer in %s is not based on answer in %s at index ' %
                  (projectid, NextQuestionCode, PrevQuestionCode) + str((df[df['FLAG'] == False].index).tolist()))
            df = createDeleteColumn(df, NextQuestionCode)
            df.loc[df['FLAG'] == False, NextQuestionCode + '_DELETE'] = 1
            df.loc[df['FLAG'] == False, NextQuestionCode] = np.nan
        df.drop(columns = ['FLAG'], inplace = True)
    elif NextQuestionCodeType == 'CHECKBOX':
        sapaColumns = [(x, y) for x in QuestionCodeDict[PrevQuestionCode] for y in QuestionCodeDict[NextQuestionCode]
                       if int(x.split('_')[-1]) == int(y.split('_')[-1])]
        df['FLAG'] = df.apply(lambda x: SAPA_CheckBox_CheckBox(x, sapaColumns), axis = 1)
        if (df['FLAG'] == True).any():
            print('Project %d Value Error (SAPA): the answer in %s is not based on answer in %s at index ' %
                  (projectid, NextQuestionCode, PrevQuestionCode) + str((df[df['FLAG'] == True].index).tolist()))
            df = createRemarksColumn(df, NextQuestionCode)
            df.loc[df['FLAG'] == True, NextQuestionCode + '_REMARKS'] = 1
            for x, y in sapaColumns:
                df.loc[df[x] == 0, y] = 0
        df.drop(columns = ['FLAG'], inplace = True)
    return df

def SAPA_CheckBox_RadioButton(x, NextQuestionCode, PrevQuestionCodeColumns):
    # is the next Question answer in the previous Question answer?
    tmp = False
    i = 0
    if pd.isnull(x[NextQuestionCode]) or x[NextQuestionCode] in [9997, 9998, 9999]:
        tmp = True
    while(tmp != True and i < len(PrevQuestionCodeColumns)):
        if pd.notnull(x[NextQuestionCode]) and x[NextQuestionCode] == int(PrevQuestionCodeColumns[i].split('_')[-1]):
            tmp = True
        i += 1
    return tmp

def SAPA_CheckBox_CheckBox(row, sapaColumns):
    # is the next Question answer list not in the previous Question answer list?
    tmp = False
    i = 0
    while(tmp != True and i < len(sapaColumns)):
        if not sapaColumns[i][0].split('_')[-1] in ['9997', '9998', '9999'] and \
                row[sapaColumns[i][0]] == 0 and row[sapaColumns[i][1]] == 1:
            tmp = True
        i += 1
    return tmp

def HAPA_RadioButton(df, projectid, PrevQuestionCode, NextQuestionCode, NextQuestionCodeType, QuestionCodeDict):
    if NextQuestionCodeType == 'RADIOBUTTON':
        if (df[PrevQuestionCode] == df[NextQuestionCode]).any():
            print('Project %d Value Error (HAPA): the answer in %s is not hidden in %s at index ' %
                  (projectid, NextQuestionCode, PrevQuestionCode) +
                  str((df[df[PrevQuestionCode] == df[NextQuestionCode]].index).tolist()))
            df = createDeleteColumn(df, NextQuestionCode)
            df.loc[df[PrevQuestionCode] == df[NextQuestionCode], NextQuestionCode+'_DELETE'] = 1
            df.loc[df[PrevQuestionCode] == df[NextQuestionCode], NextQuestionCode] = np.nan
    elif NextQuestionCodeType == 'CHECKBOX':
        df['FLAG'] = df.apply(lambda x: HAPA_RadioButton_CheckBox(x, PrevQuestionCode, NextQuestionCode, QuestionCodeDict), axis = 1)
        if (df['FLAG'] == False).any():
            print('Project %d Value Error (HAPA): the answer in %s is not hidden in %s at index ' %
                  (projectid, NextQuestionCode, PrevQuestionCode) +
                  str((df[df['FLAG'] == False].index).tolist()))
            df = createRemarksColumn(df, NextQuestionCode)
            df.loc[df['FLAG'] == False, NextQuestionCode+'_REMARKS'] = 1
            df = df.apply(lambda x: HAPA_RadioButton_CheckBox_ChangeAnswer(x, PrevQuestionCode, NextQuestionCode, QuestionCodeDict), axis = 1)
        df.drop(columns = ['FLAG'], inplace = True)
    return df

def HAPA_RadioButton_CheckBox(x, PrevQuestionCode, NextQuestionCode, QuestionCodeDict):
    # is the NextQuestionCode checkbox answer not ticked based on the PrevQuestionCode answer?
    tmp = True
    if pd.notnull(x[PrevQuestionCode]) and NextQuestionCode+'_'+str(x[PrevQuestionCode]) in QuestionCodeDict[NextQuestionCode]:
        if x[NextQuestionCode+'_'+str(x[PrevQuestionCode])] == 1:
            tmp = False
    return tmp

def HAPA_RadioButton_CheckBox_ChangeAnswer(x, PrevQuestionCode, NextQuestionCode, QuestionCodeDict):
    if x['FLAG'] == False:
        if NextQuestionCode+'_'+str(x[PrevQuestionCode]) in QuestionCodeDict[NextQuestionCode]:
            x[NextQuestionCode+'_'+str(x[PrevQuestionCode])] = 0
    return x

def HAPA_CheckBox(df, projectid, PrevQuestionCode, NextQuestionCode, NextQuestionCodeType, QuestionCodeDict):
    if NextQuestionCodeType == 'RADIOBUTTON':
        df['FLAG'] = df.apply(lambda x: HAPA_CheckBox_RadioButton(x, NextQuestionCode, QuestionCodeDict[PrevQuestionCode]), axis = 1)
        if (df['FLAG'] == False).any():
            print('Project %d Value Error (HAPA): the answer in %s is not hidden in %s at index ' %
                  (projectid, NextQuestionCode, PrevQuestionCode) + str((df[df['FLAG'] == False].index).tolist()))
            df = createDeleteColumn(df, NextQuestionCode)
            df.loc[df['FLAG'] == False, NextQuestionCode + '_DELETE'] = 1
            df.loc[df['FLAG'] == False, NextQuestionCode] = np.nan
        df.drop(columns = ['FLAG'], inplace = True)
    elif NextQuestionCodeType == 'CHECKBOX':
        hapaColumns = [(x, y) for x in QuestionCodeDict[PrevQuestionCode] for y in QuestionCodeDict[NextQuestionCode]
                       if int(x.split('_')[-1]) == int(y.split('_')[-1])]
        df['FLAG'] = df.apply(lambda x: HAPA_CheckBox_CheckBox(x, hapaColumns), axis = 1)
        if (df['FLAG'] == True).any():
            print('Project %d Value Error (HAPA): the answer in %s is not hidden in %s at index ' %
                  (projectid, NextQuestionCode, PrevQuestionCode) + str((df[df['FLAG'] == True].index).tolist()))
            df = createRemarksColumn(df, NextQuestionCode)
            df.loc[df['FLAG'] == True, NextQuestionCode + '_REMARKS'] = 1
            for x, y in hapaColumns:
                df.loc[df[x] == 1, y] = 0
        df.drop(columns = ['FLAG'], inplace = True)
    return df

def HAPA_CheckBox_RadioButton(x, NextQuestionCode, PrevQuestionCodeColumns):
    # is the next Question answer in the previous Question answer?
    i = 0
    tmp = False
    while(tmp != True and i<len(PrevQuestionCodeColumns)):
        if pd.notnull(x[NextQuestionCode]) and x[PrevQuestionCodeColumns[i]] == 1 and x[NextQuestionCode] == int(PrevQuestionCodeColumns[i].split('_')[-1]):
            tmp = True
        i += 1
    return tmp

    # tmp_list = []
    # tmp = False
    # for each in PrevQuestionCodeColumns:
    #     if not each.split('_')[-1] in ['9997', '9998', '9999'] and pd.notnull(x[each]) and x[each] == 1:
    #         tmp_list.append(int(each.split('_')[-1]))
    # if pd.isnull(x[NextQuestionCode]) or not x[NextQuestionCode] in tmp_list:
    #     tmp = True
    # return tmp

def HAPA_CheckBox_CheckBox(row, sapaColumns):
    # is the next question answer list in the previous question answer list?
    tmp = False
    i = 0
    while(tmp != True and i < len(sapaColumns)):
        if not sapaColumns[i][0].split('_')[-1] in ['9997', '9998', '9999'] and row[sapaColumns[i][0]] == 1 and row[sapaColumns[i][1]] == 1:
            tmp = True
        i += 1
    return tmp

def SQPA_RadioButton(df, projectid, PrevQuestionCode, conformingAnswer, NextQuestionCode, NextQuestionCodeType, QuestionCodeDict):
    if NextQuestionCodeType == 'RADIOBUTTON':
        if (df[~df[PrevQuestionCode].isin(conformingAnswer)][NextQuestionCode].notnull()).any():
            print('Project %d Value Error (SQPA): the respondent shouldn\'t answer %s based on answer in %s at index ' %
                  (projectid, NextQuestionCode, PrevQuestionCode) +
                  str((df[(~df[PrevQuestionCode].isin(conformingAnswer)) &
                          (df[NextQuestionCode].notnull())].index).tolist()))
            df = createDeleteColumn(df, NextQuestionCode)
            df.loc[(~df[PrevQuestionCode].isin(conformingAnswer)) & (df[NextQuestionCode].notnull()), NextQuestionCode + '_DELETE'] = 1
            df.loc[~df[PrevQuestionCode].isin(conformingAnswer), NextQuestionCode] = np.nan
        if (df[df[PrevQuestionCode].isin(conformingAnswer)][NextQuestionCode].isnull()).any():
            print('Project %d Value Error (SQPA): the respondent should answer %s based on answer in %s at index ' %
                  (projectid, NextQuestionCode, PrevQuestionCode) +
                  str((df[(df[PrevQuestionCode].isin(conformingAnswer)) &
                          (df[NextQuestionCode].isnull())].index).tolist()))
    elif NextQuestionCodeType == 'CHECKBOX':
        df['FLAG'] = df.apply(lambda x: isOneColumnNotNull(x, QuestionCodeDict[NextQuestionCode]), axis = 1)
        if (df[~df[PrevQuestionCode].isin(conformingAnswer)]['FLAG'] == True).any():
            print('Project %d Value Error (SQPA): the respondent shouldn\'t answer %s based on answer in %s at index ' %
                  (projectid, NextQuestionCode, PrevQuestionCode) +
                  str((df[(~df[PrevQuestionCode].isin(conformingAnswer)) & (df['FLAG'] == True)].index).tolist()))
            df = createRemarksColumn(df, NextQuestionCode)
            df.loc[(~df[PrevQuestionCode].isin(conformingAnswer)) & (df['FLAG'] == True),
                   NextQuestionCode + '_REMARKS'] = 1
            for each in QuestionCodeDict[NextQuestionCode]:
                df.loc[~df[PrevQuestionCode].isin(conformingAnswer), each] = np.nan
        if (df[df[PrevQuestionCode].isin(conformingAnswer)]['FLAG'] == False).any():
            print('Project %d Value Error (SQPA): the respondent should answer %s based on answer in %s at index ' %
                  (projectid, NextQuestionCode, PrevQuestionCode) +
                  str((df[(df[PrevQuestionCode].isin(conformingAnswer)) & (df['FLAG'] == False)].index).tolist()))
            df.drop(columns = ['FLAG'], inplace = True)
    return df

def SQPA_CheckBox(df, projectid, PrevQuestionCode, conformingAnswer, NextQuestionCode, NextQuestionCodeType, QuestionCodeDict):
    PrevQuestionCodeColumns = [PrevQuestionCode + '_' + str(int(x)) for x in conformingAnswer
                               if PrevQuestionCode + '_' + str(int(x)) in QuestionCodeDict[PrevQuestionCode]]
    if len(PrevQuestionCodeColumns) > 0:
        df['FLAG1'] = df.apply(lambda x: isOneColumnNotNullAndOne(x, PrevQuestionCodeColumns), axis = 1)
        if NextQuestionCodeType == 'RADIOBUTTON':
            if (df[df['FLAG1'] == 0][NextQuestionCode].notnull()).any():
                print('Project %d Value Error (SQPA): the respondent shouldn\'t '
                      'answer %s based on answer in %s at index ' % (projectid, NextQuestionCode, PrevQuestionCode) +
                      str((df[(df['FLAG1'] == 0) & (df[NextQuestionCode].notnull())].index).tolist()))
                df = createDeleteColumn(df, NextQuestionCode)
                df.loc[(df['FLAG1'] == 0) & (df[NextQuestionCode].notnull()), NextQuestionCode + '_DELETE'] = 1
                df.loc[df['FLAG1'] == 0, NextQuestionCode] = np.nan
            if (df[df['FLAG1'] == 1][NextQuestionCode].isnull()).any():
                print('Project %d Value Error (SQPA): the respondent should answer %s based on answer in %s at index ' %
                      (projectid, NextQuestionCode, PrevQuestionCode) +
                      str((df[(df['FLAG1'] == 1) & (df[NextQuestionCode].isnull())].index).tolist()))
        elif NextQuestionCodeType == 'CHECKBOX':
            df['FLAG2'] = df.apply(lambda x: isOneColumnNotNull(x, QuestionCodeDict[NextQuestionCode]), axis = 1)
            if (df[df['FLAG1'] == 0]['FLAG2'] == 1).any():
                print('Project %d Value Error (SQPA): the respondent shouldn\'t '
                      'answer %s based on answer in %s at index ' % (projectid, NextQuestionCode, PrevQuestionCode) +
                      str((df[(df['FLAG1'] == 0) & (df['FLAG2'] == 1)].index).tolist()))
                df = createRemarksColumn(df, NextQuestionCode)
                df.loc[(df['FLAG1'] == 0) & (df['FLAG2'] == 1), NextQuestionCode + '_REMARKS'] = 1
                for each in QuestionCodeDict[NextQuestionCode]:
                    df.loc[df['FLAG1'] == 0, each] = np.nan
            if (df[df['FLAG1'] == 1]['FLAG2'] == 0).any():
                print('Project %d Value Error (SQPA): the respondent should answer %s based on answer in %s at index ' %
                      (projectid, NextQuestionCode, PrevQuestionCode) +
                      str((df[(df['FLAG1'] == 1) & (df['FLAG2'] == 0)].index).tolist()))
            df.drop(columns = ['FLAG2'], inplace = True)
        df.drop(columns = ['FLAG1'], inplace = True)
    return df

def HQPA_RadioButton(df, projectid, PrevQuestionCode, conformingAnswer, NextQuestionCode, NextQuestionCodeType, QuestionCodeDict):
    if NextQuestionCodeType == 'RADIOBUTTON':
        if (df[df[PrevQuestionCode].isin(conformingAnswer)][NextQuestionCode].notnull()).any():
            print('Project %d Value Error (HQPA): the respondent shouldn\'t answer %s based on answer in %s at index ' %
                  (projectid, NextQuestionCode, PrevQuestionCode) +
                  str((df[(df[PrevQuestionCode].isin(conformingAnswer)) &
                          (df[NextQuestionCode].notnull())].index).tolist()))
            df = createDeleteColumn(df, NextQuestionCode)
            df.loc[(df[PrevQuestionCode].isin(conformingAnswer)) & (df[NextQuestionCode].notnull()),
                   NextQuestionCode + '_DELETE'] = 1
            df.loc[df[PrevQuestionCode].isin(conformingAnswer), NextQuestionCode] = np.nan
    elif NextQuestionCodeType == 'CHECKBOX':
        df['FLAG'] = df.apply(lambda x: isOneColumnNotNull(x, QuestionCodeDict[NextQuestionCode]), axis = 1)
        if (df[df[PrevQuestionCode].isin(conformingAnswer)]['FLAG'] == True).any():
            print('Project %d Value Error (HQPA): the respondent shouldn\'t answer %s based on answer in %s at index ' %
                  (projectid, NextQuestionCode, PrevQuestionCode) +
                  str((df[(df[PrevQuestionCode].isin(conformingAnswer)) & (df['FLAG'] == True)].index).tolist()))
            df = createRemarksColumn(df, NextQuestionCode)
            df.loc[(df[PrevQuestionCode].isin(conformingAnswer)) & (df['FLAG'] == True), NextQuestionCode + '_REMARKS'] = 1
            for each in QuestionCodeDict[NextQuestionCode]:
                df.loc[df[PrevQuestionCode].isin(conformingAnswer), each] = np.nan
        df.drop(columns = ['FLAG'], inplace = True)
    return df

def HQPA_CheckBox(df, projectid, PrevQuestionCode, conformingAnswer, NextQuestionCode, NextQuestionCodeType, QuestionCodeDict):
    PrevQuestionCodeColumns = [PrevQuestionCode + '_' + str(int(x)) for x in conformingAnswer
                               if PrevQuestionCode + '_' + str(int(x)) in QuestionCodeDict[PrevQuestionCode]]
    if len(PrevQuestionCodeColumns) > 0:
        df['FLAG1'] = df.apply(lambda x: isOneColumnNotNullAndOne(x, PrevQuestionCodeColumns), axis = 1)
        if NextQuestionCodeType == 'RADIOBUTTON':
            if (df[df['FLAG1'] == True][NextQuestionCode].notnull()).any():
                print('Project %d Value Error (HQPA): the respondent shouldn\'t '
                      'answer %s based on answer in %s at index ' % (projectid, NextQuestionCode, PrevQuestionCode) +
                      str((df[(df['FLAG1'] == True) & (df[NextQuestionCode].notnull())].index).tolist()))
                df = createDeleteColumn(df, NextQuestionCode)
                df.loc[(df['FLAG1'] == True) & (df[NextQuestionCode].notnull()), NextQuestionCode + '_DELETE'] = 1
                df.loc[(df['FLAG1'] == True), NextQuestionCode] = np.nan
        elif NextQuestionCodeType == 'CHECKBOX':
            df['FLAG2'] = df.apply(lambda x: isOneColumnNotNull(x, QuestionCodeDict[NextQuestionCode]), axis = 1)
            if (df[df['FLAG1'] == True]['FLAG2'] == True).any():
                print('Project %d Value Error (HQPA): the respondent shouldn\'t '
                      'answer %s based on answer in %s at index ' % (projectid, NextQuestionCode, PrevQuestionCode) +
                      str((df[(df['FLAG1'] == True) & (df['FLAG2'] == True)].index).tolist()))
                df = createRemarksColumn(df, NextQuestionCode)
                df.loc[(df['FLAG1'] == True) & (df['FLAG2'] == True), NextQuestionCode + '_REMARKS'] = 1
                for each in QuestionCodeDict[NextQuestionCode]:
                    df.loc[df['FLAG1'] == True, each] = np.nan
            df.drop(columns = ['FLAG2'], inplace = True)
        df.drop(columns = ['FLAG1'], inplace = True)
    return df

def LAPASV(df, projectid, PrevQuestionCode, NextQuestionCode, QuestionCodeDict):
    NextQuestionCodeColumnSpecify = [x+'_SPECIFY' for x in QuestionCodeDict[NextQuestionCode] if x+'_SPECIFY' in df.columns]
    df['TOTAL'] = df.apply(lambda x: sumColumns(x, NextQuestionCodeColumnSpecify), axis = 1)
    if (df[PrevQuestionCode] < df['TOTAL']).any():
        print('Project %d Value Error (LAPASV): total number of %s is bigger than %s at index ' % (projectid, NextQuestionCode, PrevQuestionCode) +
              str((df[df[PrevQuestionCode] < df['TOTAL']].index).tolist()))
    df.drop(columns = ['TOTAL'], inplace = True)
    return df