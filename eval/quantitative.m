ntopics = [9, 14];
step = 8;
truth = importdata('MSRCv1.data');

cmap = [0 0 0; 128 0 0; 0 128 0; 128 128 0; 0 0 128; 128 0 128; 0 128 128; ...
    128 128 128; 64 0 0; 192 0 0; 64 128 0; 192 128 0; 64 0 128; 192 0 128];
obj = {'void', 'building', 'grass', 'tree', ...
    'cow', 'horse', 'sheep', 'sky', 'mountain', ...
    'aeroplane', 'water', 'face', 'car', 'bicycle'};
tmat = [129*129; 129; 1];
umap = cmap * tmat;
groundtopic = 14;

dataset = '../MSRCv1/';
N = length(truth.textdata);

if ~exist('truth.mat', 'file')
    ground = [];
    for i = 1:N
        filename = truth.textdata{i, 2};
        index = strfind(filename, '.');
        filename = [dataset filename(1:index-1) '_GT' filename(index:end)];
        disp(filename);
        im = double(imread(filename));

        width = truth.data(id, 1);
        height = truth.data(id, 2);
        im = reshape(im, width*height, 3) * tmat;

        m = abs(bsxfun(@minus, im, umap'));
        [a, index] = min(m, [], 2);
        ground = [ground index];
    end
    save('truth.mat', 'ground');
else
    load('truth.mat');
end

rerun = 0;

for ntopic = ntopics
    matfile = ['data-topic-' num2str(ntopic) '.mat'];
    disp(matfile);
    if ~exist(matfile, 'file') || rerun

        result = [];
        folder = ['msrc-topic-' num2str(ntopic)];

        for id = 1:N
            disp(id);
            filename = [folder '/' num2str(id) '.data'];

            data = importdata(filename);

            width = truth.data(id, 1);
            height = truth.data(id, 2);

            im = zeros(height, width);
            for i = 1:length(data)
                x = data(i, 1) + 1;
                y = data(i, 2) + 1;
                for dx = -step/2:step/2
                    if x + dx > width, break, end
                    for dy = -step/2:step/2
                        if y + dy > height, break, end
                        im(y + dy, x + dx) = data(i,3) + 1;
                    end
                end
            end

            result = [result im(:)];

        end
        save(matfile, 'result');
    else
        load(matfile);
    end
    
    % validate
    tp = zeros(groundtopic, 1);
    fa = zeros(groundtopic, 1);
    for i = 1:groundtopic
        index = find(ground == i);
        label = mode(result(index));
        fprintf(1, 'i = %d, label = %d\n', i, label);
        gi = (ground == i);
        ri = (result == label) .* (ground ~= 1);
        tp(i) = sum(sum(gi .* ri)) / sum(sum(gi));
        fa(i) = sum(sum((1-gi) .* ri)) / sum(sum(ri));
    end
    dlmwrite(['result-topic-' num2str(ntopic) '.txt'], [(1:groundtopic-1)' tp(2:end) fa(2:end)], '\t');
end