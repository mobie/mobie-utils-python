// Set resolution for a tif file
setBatchMode(true);

// we expect an input string in the following format
// filename,nslices,width,height,depth
args = getArgument();
args = split(args, ",");

filename = args[0];
nslices = args[1];
nchannels = args[2]

w = args[3];
h = args[4];
d = args[5];

// assemble the properties string

props = "channels=" + nchannels + " slices=" + nslices + " frames=1 unit=micrometers pixel_width=" + w + " pixel_height=" + h + " voxel_depth=" + d;

stack_args = "order=xyzct channels=" + nchannels + " slices=" + nslices + " frames=1 display=Color"

// NOTE this does not work with relative paths
open(filename);

run("Stack to Hyperstack...", stack_args);
run("Properties...", props);
saveAs("Tiff", filename);
close(filename);
