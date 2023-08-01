import pandas as pd
from Dataset import Base, Raw, Matrix
import copy
import variables.dict as dct
import variables.PERSONAL_setup.my_dict as stp_dct
import variables.var_column as clmn


def classify(original_mtx_cashflow, any_mtx_budget_classification):
    any_mtx_cashflow = copy.deepcopy(original_mtx_cashflow)
    any_mtx_cashflow = classify_specific_row(any_mtx_cashflow, any_mtx_budget_classification)
    any_mtx_cashflow = classify_description_pattern(any_mtx_cashflow, any_mtx_budget_classification)
    # classify_specific_row
    # classify_specific_description
    # classify_aproximated_description

    return any_mtx_cashflow


def classify_specific_row(any_mtx_cashflow, original_budget_classification):
    any_mtx_budget_classification = copy.deepcopy(original_budget_classification)
    any_mtx_budget_classification.filter_based_on_column(any_column=clmn.budget_strategy, value_list=['specific row'])

    any_mtx_cashflow.merge_datamatrix(right_datamatrix=any_mtx_budget_classification,
                                      desired_column_list=[clmn.budget, clmn.budget_strategy],
                                      left_on_list=[clmn.date, clmn.description, clmn.bank, clmn.account_type],
                                      right_on_list=[clmn.date, clmn.description, clmn.bank, clmn.account_type],
                                      reset_right_index=True, reset_left_index=True, drop_right_on_list=False)

    return any_mtx_cashflow


def filter_classified_rows(original_mtx_cashflow):
    mtx_classified = copy.deepcopy(original_mtx_cashflow)
    mtx_classified.filter_nan_on_column(any_column=clmn.budget, keep_nan=False)

    mtx_not_classified = copy.deepcopy(original_mtx_cashflow)
    mtx_not_classified.filter_nan_on_column(any_column=clmn.budget, keep_nan=True)
    return mtx_classified, mtx_not_classified


def classify_description_pattern(original_mtx_cashflow, original_budget_classification):
    mtx_classified_before, mtx_not_classified = filter_classified_rows(original_mtx_cashflow)
    any_mtx_budget_classification = copy.deepcopy(original_budget_classification)
    any_mtx_budget_classification.filter_based_on_column(any_column=clmn.budget_strategy,
                                                         value_list=['description pattern'])
    pattern_list = any_mtx_budget_classification.get_terms_of_a_column(any_column=clmn.description, reset_index=True)
    classification_list = any_mtx_budget_classification.get_terms_of_a_column(any_column=clmn.budget, reset_index=True)

    print('cash 50')
    print(pattern_list)
    print(classification_list)
    ii = 0

    mtx_classified_after = copy.deepcopy(original_mtx_cashflow)
    mtx_classified_after.dataframe = pd.DataFrame() # refactor
    for any_pattern in pattern_list:
        mtx_not_classified.search_string_column_for_pattern(target_column=clmn.description,
                                                                               pattern=any_pattern,
                                                                               any_column=clmn.budget,
                                                                               value=classification_list[ii])
        mtx_classified_after_to_append, mtx_not_classified = filter_classified_rows(mtx_not_classified)
        mtx_classified_after.concat_base_dataset(mtx_classified_after_to_append)
        ii = ii + 1

    mtx_classified_after.assign_constant_to_column(any_clmn=clmn.budget_strategy, value='description pattern')

    any_mtx_cashflow = copy.deepcopy(original_mtx_cashflow)
    any_mtx_cashflow.dataframe = pd.DataFrame() # refactor
    any_mtx_cashflow.concat_base_dataset_list(any_basedataset_list=[mtx_classified_before,
                                                                    mtx_classified_after, mtx_not_classified])
    # filter description pattern on mtx_budget_classif
    # for every row of mbc seach cash flow for patterns
    # append mtx_classified to mtx_cashflow
    return any_mtx_cashflow


def load_cashflow(any_stp_dict):
    raw_chase = Raw.RawDataset.load_from_json('raw_chase', root=stp_dct.setup_dict[dct.root_folder],
                                                                   folder=stp_dct.setup_dict[dct.json_folder])
    raw_chase_credit = Raw.RawDataset.load_from_json('raw_chase_credit', root=stp_dct.setup_dict[dct.root_folder],
                                                                   folder=stp_dct.setup_dict[dct.json_folder])

    mtx_cashflow = Matrix.DataMatrix.load_from_json('cashflow', root=stp_dct.setup_dict[dct.root_folder],
                                                                   folder=stp_dct.setup_dict[dct.json_folder])

    raw_chase.load_dataframe(treat_date=False, load_from_spreadsheet=True, load_manually_from_dataframe=False)
    raw_chase_credit.load_dataframe(treat_date=False, load_from_spreadsheet=True, load_manually_from_dataframe=False)
    mtx_cashflow.concat_base_dataset_list([raw_chase, raw_chase_credit])
    mtx_cashflow.apply_standard_index()


    mtx_budget_classification = load_budget_classification(any_stp_dict)
    mtx_cashflow = classify(original_mtx_cashflow=mtx_cashflow, any_mtx_budget_classification=mtx_budget_classification)
    # print('cash 44')
    # print(mtx_cashflow.dataframe)
    mtx_cashflow.sort_column(any_column=clmn.date, ascending=False, reset_index=True)
    mtx_cashflow.assure_column_integrity()
    mtx_cashflow.write(any_stp_dict)
    return


def load_budget_classification(any_stp_dict):
    any_mtx_budget_classification = Matrix.DataMatrix.load_from_json(name='budget_classification',
                                                        root=any_stp_dict[dct.root_folder],
                                                        folder=any_stp_dict[dct.json_folder])
    any_mtx_budget_classification.load_dataframe_from_family(base_dataset_family_name='raw_budget_classification',
                                                     any_stp_dict=any_stp_dict,
                                                     treat_date=False, load_all_files_within_folder=False,
                                                     load_all_sheets_on_spreadsheet=True)
    any_mtx_budget_classification.write(any_stp_dict)
    return any_mtx_budget_classification


if __name__ == "__main__":
    pd.set_option('display.max_columns', 50)
    pd.set_option('display.max_rows', 70000)
    load_cashflow(any_stp_dict=stp_dct.setup_dict)