"""
A collection of utilities for working with Pandas objects
"""
import warnings

import numpy as np
import pandas as pd
from pandas.core.dtypes.api import (is_bool_dtype, is_categorical_dtype, is_string_dtype, is_numeric_dtype,
                                    is_datetime64_any_dtype, is_timedelta64_dtype, is_timedelta64_ns_dtype,
                                    is_interval_dtype)


def _make_unary_func(left, right, binary_func, ignore_index, ignore_columns):
    """
    Private Decorator for apply.columnwise which wraps binary_func
    """
    # the basic idea is that we create and return a decorated unary version of the user-supplied
    # binary_func which handles data selection from left, right based on index/column choices
    # NB: this version only works if the shapes of left, right match
    # this function is written from the right.apply(unary_func) perspective which means
    # all label-aware index selection from left uses index labels from right to match the corresponding
    # values that will be passed later by right.apply()
    if ignore_index is False:
        if ignore_columns is False:
            # use matching index labels and matching column labels
            def _unary_func(x):
                _l = left.loc[x.index, x.name]
                return binary_func(_l, x)
        else:
            # use the column position and matching index values
            def _unary_func(x):
                _l = left.loc[x.index, left.columns[right.columns.get_loc(x.name)]]
                return binary_func(_l, x)
    else:
        if ignore_columns is False:
            # use the column label and ignore the index by comparing underlying value arrays
            def _unary_func(x):
                _l = left[x.name].values
                _r = x.values
                return binary_func(_l, _r)
        else:
            # use the column position and ignore the index by comparying underlying value arrays
            # most low-level use case using both underlying ndarrays
            def _unary_func(x):
                _l = left[left.columns[right.columns.get_loc(x.name)]].values
                _r = x.values
                return binary_func(_l, _r)
    return _unary_func


def apply_columnwise(left, right, binary_func, ignore_index=False, ignore_columns=False):
    """
    Apply a binary function to pairs of matching columns from `left` and `right` pd.DataFrames. Use ignore_index and
    ignore_columns flags to determine how to find matching labels while performing data selection prior to passing into
    `binary_func`.  Essentially performs an "each" operation using `binary_func` along axis=1 of `left` and `right`.

    Result is a pd.DataFrame with dimensions containing the matched rows/columns in `right`

    By default index and column labels are used to select/loc matching pd.Series from `left` and `right` and pass
    to `binary_func`

    When ignoring index or column labels, the shapes of `left` and `right` must match along the corresponding axis or
    a ValueError is raised.  The special cases of a single column in `left` (`right`) and m > 1 columns in `right`
    (`left`) is allowed when ignore_columns is True.  In this case, the `binary_func` is applied to the same single
    column in `left` and each column in `right` i.e. an "each right /:" operation or the same column `right` and
    each column in `left`, i.e. an "each left \:" operation.

    Parameters
    ----------
    left : pd.Series, pd.DataFrame
    right : pd.Series, pd.DataFrame
    binary_func : function of 2-variables
        must take exactly two pd.Series arguments and return a single pd.Series
    ignore_index : bool
        When True will ignore index labels and apply the function to the underlying value arrays of both columns
        which requires length of `left` and `right` to match
    ignore_columns : bool
        When True will ignore column labels and apply binary_func to pairs of columns with matching integer-index
        locations. When True, either `left` and `right` must have matching columns or `left` must be a single column
        which will be passed to `binary_func` with each column from `right`

    Returns
    -------

    """
    if isinstance(left, pd.Series):
        left = left.to_frame()
    if isinstance(right, pd.Series):
        right = right.to_frame()
    if not isinstance(left, pd.DataFrame) or not isinstance(right, pd.DataFrame):
        raise ValueError('left and right must be pd.Series or pd.DataFrames!')
    if not callable(binary_func):
        raise ValueError('binary_func must be a callable!')
    if left.shape[0] != right.shape[0]:
        # different number of rows can only work in certain situations
        if ignore_index is True:
            raise ValueError("left, right must have same number of rows when ignoring index labels")
        else:
            # validate there are at least 1 index labels in common
            # otherwise preempt the future KeyError
            if len(left.index.intersection(right.index)) == 0:
                raise ValueError("Cannot apply function! left, right do not share any index labels in common!")
    if left.shape[1] != right.shape[1]:
        # different number of columns is only valid in certain situations
        if ignore_columns is True:
            # the special case(s) of 1 column in left (right) and m > 1 in right (left) is allowed
            # all other pairs of mismatched column counts cannot be consistently interpreted
            if left.shape[1] == 1:
                # in this case, we need to broadcast left up to the size of right
                left = pd.DataFrame([left.iloc[:, 0]] * right.shape[1]).reset_index(drop=True).T
            elif right.shape[1] == 1:
                right = pd.DataFrame([right.iloc[:, 0]] * left.shape[1]).reset_index(drop=True).T
            else:
                raise ValueError("Cannot apply function! left or right must have a single column when "
                                 "left, right have different number of columns and ignoring column labels")
        else:
            # determine if any columns in common and only use those
            _cols_in_common = left.columns.intersection(right.columns)
            if len(_cols_in_common) == 0:
                raise ValueError("Cannot apply function! left, right do not share any column labels in common")
            left = left[_cols_in_common]
            right = right[_cols_in_common]
    _final_unary_func = _make_unary_func(left, right, binary_func, ignore_index, ignore_columns)
    result = right.apply(_final_unary_func)
    return result


def dtype_specific_binary(left, right, numerics, datetimes, bools, strings, categoricals, intervals, errors='ignore'):
    """
    A low-level base binary function to perform different binary operations based on the dtypes of left, right inputs.
    Can be used with functools.partial to create a custom binary function that can be passed to apply_columnwise or
    higher order compare_values utilities found elsewhere in this module.  See examples for more details.

    This function supports 6 distinct groups of pandas dtypes which are validated using a corresponding set of helpers
    provided by the pandas.core.dtypes API:

        1) numeric - is_numeric_dtype
        2) datetime-like - is_datetime64_any_dtype or istimedelta64_dtype
        3) bool - is_bool_dtype
        4) string - is_string_dtype
        5) categorical - is_categorical_dtype
        6) is_interval_dtype

    If no supported dtype is matched, or `left` & `right` do not have matching dtypes apd.Series of NaN values is
    returned unless errors='raise' in which case a ValueError is raised.

    Parameters
    ----------
    left : pd.Series, pd.DataFrame, np.ndarray
    right : pd.Series, pd.DataFrame, np.ndarray
    numerics : binary callable
        applied to numeric dtypes
    datetimes : binary callable
        applied to datetime-like objects
    bools : binary callable
        applied to bool dtypes
    strings : binary callable
        applied to string-like dtypes
    categoricals : binary callable
        applied to Categorical dtype
    intervals : binary callable
        applied to Interval dtype
    errors : str
        default 'ignore' issues warning and returns NaNs when dtype of left, right do not match
        if 'raise' is passed, will raise ValueError in such cases

    Returns
    -------
    result of applying a specific binary callable to `left` and `right` inputs based on dtype

    """
    _ld = left.dtype
    _rd = right.dtype
    if is_numeric_dtype(_ld) and is_numeric_dtype(_rd):
        return numerics(left, right)
    elif ((is_datetime64_any_dtype(_ld) or is_timedelta64_dtype(_ld) or is_timedelta64_ns_dtype(_ld)) and
          (is_datetime64_any_dtype(_rd) or is_timedelta64_dtype(_rd) or is_timedelta64_ns_dtype(_rd))):
        return datetimes(left, right)
    elif is_bool_dtype(_ld) and is_bool_dtype(_rd):
        return bools(left, right)
    elif is_string_dtype(_ld) and is_string_dtype(_rd):
        return strings(left, right)
    elif is_categorical_dtype(_ld) and is_categorical_dtype(_rd):
        return categoricals(left, right)
    elif is_interval_dtype(_ld) and is_interval_dtype(_rd):
        return intervals(left, right)
    else:
        # by default when dtypes are mismatched we issue a warning and return NaNs
        # raise if user requires it
        if errors == 'raise':
            raise ValueError(f"left and right do not have matching supported dtypes: {_ld.name}, {_rd.name}")
        else:
            warnings.warn(f"left: {left.name}, {_ld.name} and right: {right.name}, {_rd.name}"
                          f" do not have comparable dtypes, returning NaNs")
            return pd.Series(np.nan, index=right.index)
