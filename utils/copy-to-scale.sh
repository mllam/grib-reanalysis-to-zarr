DEST_DIR=/dmidata/projects/cloudphysics/danra/data

# get version number to copy from command line
if [ $# -eq 0 ]
  then
    echo "usage: copy-to-scale.sh <version>"
    exit 1
else
    VERSION=$1
    echo "Copying version $VERSION to scale.dmi.dk"
fi

rsync --progress -r "${VERSION}" "${DEST_DIR}/${VERSION}/"
cp ~/git-repos/danra_to_zarr/CHANGELOG.md "${DEST_DIR}/"
