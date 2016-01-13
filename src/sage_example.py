import numpy as np


class SAGE(object):

    def __init__(self):
        print("initialise 'local' disk store")
        self.disk_store = {}

    def put(self, fid, data):
        """
        Basic Functionality
        """
        print("Recieve data to storage machine")
        local_data = data
        print("put data to disk")
        self.disk_store[fid] = local_data

    def get(self, fid):
        """
        Basic Functionality
        """
        print("retrieve data from disk")
        local_data = self.disk_store[fid]
        print("transmit data from storage")
        return local_data

    def get_slice(self, fid, slice_values):
        """
        Possible Function Offload 
        Very useful for the HDF5 implementation
        """
        print("retrieve data from disk")
        local_data = self.disk_store[fid]
        print("slice data on the storage")
        sliced_local_data = local_data[slice_values]
        print("transmit sliced data from storage")
        return sliced_local_data

    def get_subtract(self, fid_A, fid_B):
        print("retrieve data A from disk")
        local_data_A = self.disk_store[fid_A]
        print("retrieve data B from disk")
        local_data_B = self.disk_store[fid_B]
        print("Make the subtraction on the storage")
        subtracted_local_data = local_data_A - local_data_B
        print("transmit subtracted data from storage")
        return subtracted_local_data

    def remote_subtract(self, fid_A, fid_B, fid_result):
        print("retrieve data A from disk")
        local_data_A = self.disk_store[fid_A]
        print("retrieve data B from disk")
        local_data_B = self.disk_store[fid_B]
        print("Make the subtraction on the storage")
        subtracted_local_data = local_data_A - local_data_B
        print("Put data back to the local storage")
        self.disk_store[fid_result] = subtracted_local_data

    def remote_divide(self, fid_A, fid_B, fid_result):
        print("retrieve data A from disk")
        local_data_A = self.disk_store[fid_A]
        print("retrieve data B from disk")
        local_data_B = self.disk_store[fid_B]
        print("Make the subtraction on the storage")
        divided_local_data = local_data_A / local_data_B
        print("Put data back to the local storage")
        self.disk_store[fid_result] = divided_local_data



# set up connection to SAGE
sage = SAGE()

# put some data into the SAGE blockstore
sage.put(1, np.ones(shape=(5, 10, 20), dtype=np.int16))
sage.put(2, np.ones(shape=(1, 10, 20), dtype=np.int16)+1)
sage.put(3, np.ones(shape=(1, 10, 20), dtype=np.int16)+2)

# get some of that data
print sage.get(1).shape
print sage.get(2).shape
print sage.get(3).shape

# get sliced data
print sage.get_slice(1, (slice(2, 3, 1), slice(None), slice(None))).shape

# get subtracted data
sub = sage.get_subtract(1, 2)
print sub.shape
print sub

# realistic process dealing with applying corrections to some collected
# xray data, but taking only a small portion of it

# the correction we want to do is (1 - 2) / (3 - 2)

# so first do the calculation on the storeage
sage.remote_subtract(1, 2, 4)
sage.remote_subtract(3, 2, 5)
sage.remote_divide(4, 5, 6)

# then retrieve the data locally
result = sage.get(6)
print result.shape
print result



