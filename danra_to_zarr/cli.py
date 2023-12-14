from .main import create_zarr_dataset

if __name__ == "__main__":
    fp_out = "example_danra_data.zarr"
    rechunk_to = dict(time=4, x=512, y=512)
    create_zarr_dataset(fp_out=fp_out, rechunk_to=rechunk_to)
