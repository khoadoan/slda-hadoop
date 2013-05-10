ntopics = 14;
step = 8;
folder = ['msrc-topic-' num2str(ntopics)];
flist = dir([folder '/*.data']);

truth = importdata('MSRCv1.data');

outputfolder = [folder '-images'];
if ~exist(outputfolder, 'dir')
    mkdir(outputfolder);
end
C = jet(ntopics+1);

for i = 1:length(flist)
    disp(flist(i).name);
    name = flist(i).name;

    filename = [folder '/' name];
    data = importdata(filename);
    
    index = strfind(name, '.');
    id = str2num(name(1:index-1));
    
    width = truth.data(id, 1);
    height = truth.data(id, 2);

    im = zeros(height, width, 3);
    for i = 1:length(data)
        x = data(i, 1) + 1;
        y = data(i, 2) + 1;
        for dx = -step/2:step/2
            if x + dx > width, break, end
            for dy = -step/2:step/2
                if y + dy > height, break, end
                im(y + dy, x + dx, :) = C(data(i,3) + 1, :)';
            end
        end
    end

    outputname = [outputfolder '/' name(1:index) 'bmp'];
    imwrite(im, outputname, 'bmp');
end