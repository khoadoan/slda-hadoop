reset
set style data histogram
set style histogram cluster gap 1
set style fill solid border -1
set boxwidth 0.8
set xlabel "Object Classes"
set ylabel "Detection rate / False alarm"
set xrange [-1:13]
set yrange [0:1.2]
set xtics ("building" 0, "grass" 1, "tree" 2, "cow" 3, "horse" 4, "sheep" 5, "sky" 6, "mountain" 7, "aeroplane" 8, "water" 9, "face" 10, "car" 11, "bicycle" 12) rotate by -45
set grid
set title "LDA Performance on MSRCv1 (#topics = 14)"
p "result-topic-14.txt" u 2 title 'Detection Rate', '' u 3 title 'False Alarm' 
