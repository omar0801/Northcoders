import pandas as pd

def create_dim_date(start_date = '2000-01-01', end_date='2000-12-31'):

    try:
        df_start_date = pd.Timestamp(start_date)  
        df_end_date = pd.Timestamp(end_date)
        
        start_str_date = df_start_date.strftime('%Y%m%d')
        start_num_date = pd.to_numeric(start_str_date)
        end_str_date = df_end_date.strftime('%Y%m%d')
        end_num_date = pd.to_numeric(end_str_date)

        if end_num_date < start_num_date :
            return "End date must be greter then Start date"

        df = pd.DataFrame({"Date": pd.date_range(start_date, end_date)})
        df["Year"] = df.Date.dt.year
        df["Month"] = df.Date.dt.month_name()
        df["Day"] = df.Date.dt.day
        df["Day_Of_Week"] = df.Date.dt.day_of_week
        df["Day_Name"] = df.Date.dt.day_name()
        df["Quarter"] = df.Date.dt.quarter
        df.set_index(['Date'], inplace=True)
        return df
    except:
        return "Incorrect Date"
        
#print(create_dim_date('200990-01-01', '2001-02-08'))