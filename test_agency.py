from etl.google_sheet_getters import get_google_sheet
import numpy as np
import pandas as pd

truth = get_google_sheet('1jDehUWW5n1UeU9Z8bsAscGVUiSj8FYOlzcEXBGS1Y1g', 'Export', 'C1:AY22')
truth.index = truth.iloc[:, 0] + ' ' + truth.iloc[:, 1]
truth = truth.iloc[:, 2:]

test = get_google_sheet('1BuR-tIVZBFaxBAoGdu8DHF2v-hhW5BePtOjpxnmpd60', 'data', range='B2:23')
test = test[truth.columns]


error = np.ndarray(truth.shape)
for icol in range(truth.shape[1]):
    for irow in range(truth.shape[0]):
        try:
            test_value = float(test.values[irow, icol])

            true_str = truth.values[irow, icol]
            if '%' in true_str:
                true_str = str(float(true_str.replace('%', ''))/100)

            true_str = true_str.replace(',', '')

            true_value = float(true_str)

            if true_value == 0 and test_value == 0:
                error_value = 0
            else:
                error_value = (test_value - true_value) / true_value
            error[irow, icol] = error_value
        except Exception as e:
            error[irow, icol] = np.nan

error_df = pd.DataFrame(error, index=test.index, columns=test.columns)
error_df.to_excel('error.xlsx')
