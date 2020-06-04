import time
import dask_cudf
import os
print("\nImporting Completed")

### Loading time wih cudf DataSet Higgs
for run in range(0,10):
    # os.system("nvidia-smi > loading_Higgs_"+str(run)+".txt")
    start = time.time()
    df = dask_cudf.read_csv("HIGGS.csv")
    print(df.shape)
    end = time.time()
    print("Loading Time readcsv Higgs = {}".format(end-start))

### Loading time wih cudf DataSet merchant
for run in range(0,10):
    # os.system("nvidia-smi > loading_merchant_"+str(run)+".txt")
    start = time.time()
    df = dask_cudf.read_csv("historical_transactions.csv")
    end = time.time()
    print("Loading Time readcsv merchant = {}".format(end-start))

### GroupBy in the data with cudf DataSet Higgs
for run in range(0,10):
    # os.system("nvidia-smi > GroupBy_Higgs_"+str(run)+".txt")
    df = dask_cudf.read_csv("HIGGS.csv")
    start = time.time()
    df = df.groupby("-6.350818276405334473e-01").mean()
    end = time.time()
    print("Loading Time groupby Higgs = {}".format(end - start))

### GroupBy in the data with cudf DataSet Merchant
for run in range(0,10):
    # os.system("nvidia-smi > GroupBy_merchant_"+str(run)+".txt")
    df = dask_cudf.read_csv("historical_transactions.csv")
    start = time.time()
    df = df.groupby("card_id").mean()
    end = time.time()
    print("Loading Time groupby merchant = {}".format(end - start))
