// Set resolution for a tif file
setBatchMode(true);

// we expect an input string in the following format
// filename,nslices,width,height,depth
args = getArgument();
args = split(args, ",");

filename = args[0];
nslices = args[1];
w = args[2];
h = args[3];
d = args[4];

// assemble the properties string
props = "channels=1 slices=" + nslices + " frames=1 unit=micrometers pixel_width=" + w + " pixel_height=" + h + " voxel_depth=" + d;

// NOTE this does not work with relative paths
open(filename);

run("Properties...", props);
saveAs("Tiff", filename);
close(filename);
