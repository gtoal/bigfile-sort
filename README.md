# bigfile-sort
This is a general sorting program similar to gnu sort which is optimised to use available memory as efficiently as possible in order to sort very large text files reasonably quickly.

I wrote this a long time ago (back in the 80's) and offered it to the gnu sort maintainer but never heard back from them and so I used it personally on some projects but didn't do anything else with it such as post it on comp.sources.  I recently had to do some manipulation of very large text files and found this old code in my archives which did the job better than the current tools I had available on my system, so I thought that perhaps its not too late to share it.  The beauty of C code is that it works as well in 2023 as it did in 1988 if it was written with portability in mind at the time; I'm not going to do any clean up or mods to release it here.  The space management that this code does is fairly well documented as comments.

The trick is to grab all available memory as a block and then do as much sorting as can be done within that block, followed by external merging of the sorted sub-blocks.  GNU sort (at least back in '88!) allocated memory in line-sized mallocs and could run out of memory during a sort.

This sort includes a few of the options from unix sort.  It's biggest interface difference is that by default it sorts a file in place to minimize the amount of extra disk space needed for temporary files - something that was important back in the 80's but not such an issue nowadays.

PostScript: I wrote this back in the days of 32-bit address space.  Apparently what constitutes a 'big' text file has moved on since then - I was surprised to see that a simple listing of file names on my backup drive was over 5Gb.  So if you came here looking to sort a truly huge text file (>= 4Gb) you'ld better come back later after I've updated this code to handle it :-)
