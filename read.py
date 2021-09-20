import pandas as pd

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)

df = pd.read_csv('beamlineforschools.cern_failed_links.csv',sep='\t')

print(df)#.BrokenUrlFoundInPage,'\n\n',df.WithTextValue,'\n\n',df.TheLinkThatIsBroken)
