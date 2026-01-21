import polars as pl
import polars.selectors as cs

import config.config_paths as config
import config.set_params as params

env = config.Fixed_locations
param = params.Update_param
displ = params.Display_ind_param

year = param.ANNUAL_DATE

index = param.IDX_E_COL_NAME
index_type = param.IDX_TYPE_COL_NAME
earnings_type = param.E_TYPE_COL_NAME
earnings_metric = param.E_METRIC_COL_NAME


def read():
    # for SP500, eps and p/e data, no real estate data
    ind_df = pl.read_parquet(env.OUTPUT_IND_ADDR)\
               .filter(
                   (pl.col(index_type) == param.SP500)
                   )\
               .drop(cs.matches(param.E_COLS_DROP[0]),
                     pl.col(index_type))\
               .sort(by= year)
    
    # rep (EARN_TYPES[1]) E (index) is not null
    years_act_earnings = pl.Series(
              ind_df.filter(
                  (pl.col(earnings_type) == param.EARN_TYPES[1]) &
                  (pl.col(index).is_not_null())
                  )
               .select(year)
              ).to_list()
        
    # marks entries in year with estimated E
    ind_df = ind_df.with_columns(
                        pl.when(pl.col(year)
                                .is_in(years_act_earnings))
                          .then(pl.col(year))
                          .otherwise(pl.col(year) + 'E')
                          .alias(year))
    
    return ind_df
