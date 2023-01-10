import duckdb
import pandas as pd
import streamlit as st

st.header("Please upload a parquet file")

uploaded_file = st.file_uploader("Choose a file", type=['parquet'])
if uploaded_file is not None:
    df = pd.read_parquet(uploaded_file)
    st.markdown('## preview of file:')
    st.dataframe(df.head(5))
    col = list(df.columns)

    st.markdown('Please identify the following columns by selecting from dropdown:')

    ndc_col = st.selectbox("NDC COLUMN", col)
    qty_col = st.selectbox("NUMBER OF UNITS FOR PRESCRIPTION COLUMN", col)
    dos_col = st.selectbox("DATE PRESCRIPTION WAS FILLED", col)
    total_price = st.selectbox('PRICE CHARGED COLUMN', col)
    nadac_df = st.number_input('Input a dispensing fee per NADAC presciption')

    if st.button('Process'):
        df.rename(columns={ndc_col: 'ndc', qty_col: 'qty', dos_col: 'dos', total_price: 'current_price'},inplace=True)

        nadac = pd.read_parquet('nadac.parquet')

        def add_nadac(data, nadac):
            con = duckdb.connect()
            sql = """
               SELECT a.*,
               round(nadac_per_unit * qty,2) as nadac
               FROM data a
               left join nadac b
               on a.ndc = b.ndc
               and a.dos >= b.effective_date
               and a.dos <= b.end_date
               """
            df = con.execute(sql).df()
            return df
        df = add_nadac(df, nadac)
        df = df.loc[~(df.nadac.isnull())]
        str("{:,}".format(len(df)))
        st.write(f"A total of {str('{:,}'.format(len(df)))} claims where matched to a NADAC")




        df = df.assign(
            nadac_df = lambda x: nadac_df
        )


        df = df.groupby('Product_Name').agg(prescription_count = ('Product_Name','count'),
                                            current_charge = ('current_price', 'sum'),
                                            nadac = ('nadac','sum'),
                                            nadac_df = ('nadac_df','sum')
                                            )
        df = df.assign(
            nadac_total = lambda x: x.nadac + x.nadac_df
        )

        current_total = '$' + str("{:,}".format(round(df.current_charge.sum())))
        nadac_total = '$' + str("{:,}".format(round(df.nadac_total.sum())))
        diff = '$' + str("{:,}".format(round(df.nadac_total.sum() - df.current_charge.sum())))
        percent = "{:.0%}".format((df.current_charge.sum() - df.nadac_total.sum()) / df.current_charge.sum()*-1)


        col1,col2,col3 = st.columns(3)
        with col1:
            st.metric(label='Current Charge', value= current_total,delta = None)

        with col2:
            st.metric(label='NADAC DELTA', value=diff,delta = percent)

        with col3:
            st.metric(label='NADAC Charge', value= nadac_total,delta = None)

        with st.expander('Click to view data'):
            st.dataframe(df)
