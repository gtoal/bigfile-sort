/**************************************************************************
  Program:   sort.c
  Purpose:   sort lines of text (with all kinds of options).
  Copyright: 1989 Free Software Foundation
  Created:   December 1988.
  Authors:   Mike Haertel, Graham Toal.

  This program is free software; you can redistribute it and/or modify it under
  the terms of the GNU General Public License as published by the Free
  Software Foundation; either version 1, or (at your option) any later
  version.

  This program is distributed in the hope that it will be useful, but WITHOUT
  ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
  FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for
  more details.

  You should have received a copy of the GNU General Public License along with
  this program; if not, write to the Free Software Foundation, Inc., 675
  Mass Ave, Cambridge, MA 02139, USA.

  The author may be reached (Email) at the address mike@ai.mit.edu, or (US
  mail) as Mike Haertel c/o Free Software Foundation.

  *** This source was originally released by Mike Haertel for the GNUish MSDOS
  project.  He pointed out at the time that it was very much under development.
  This version has been hacked about a lot by Graham Toal.  If you want the
  real GNU sort, please contact Mike Haertel.  This file is currently under
  development -- i.e. it does not do anything useful.  Please do not pass
  this around in the guise of an official FSF product.  However under the
  terms of the GNU Licence I am making what work I have done on this available
  to anyone who may find it useful. ***

  I have marked the clever bits of code, which are Mike Haertels, with his
  initials, just so readers won't blame him for the grotty bits, which are
  mine :-)  [Or at least any procedures where the code is mostly his and
  not hacked about too badly by me -- this seemed easier that putting ownership
  comments on every single line :-)]

  Graham Toal may be reached at gtoal@edinburgh.ac.uk (via nsfnet-relay.ac.uk
  or ukc.ac.uk), or if all routes have failed, at gtoal@vangogh.berkeley.edu

                             *===*===*===*
***************************************************************************/

/*
 * Working notes: Unix sort already uses options y & z -- remove ours.
 * Implement unix -y<kmem> option: tells how much phys mem to use. -y0 means
 * minim, -y means max(*).  This is useful on VM systems as we don't want to
 * stray outside our working set.  On physical memory systems, we probably
 * want to take as much as possible. Unless it is a multitasking phys mem
 * system I suppose :-( Also -z<recsz> for longest line length.  Not that we
 * can do anything about -- we're allowing the longest lines we can anyway...
 * What is also sub-optimal is that if sorting > 1 file, we sort the files
 * individually & merge the results.  Would be better -- if all the files fit
 * in store -- to either merge beforehand, _then_ sort, or sort but keep in
 * store for the merge rather than write back to a temp file. (*) I think the
 * mach kernel has hooks for finding out the current working-set size. If
 * that feature isn't present, assume max vm size = max phys size :-(
 * Remember to add -u option! (Note optimisation caveats in sort.h...)
 */

/**************************************************************************

We used GNU's sort clone as a central resource in a database indexing
project; Although there were many problems with GNUsort which made life
difficult for us, its core sorting algorithm and features were superb.

To solve the problems we had with GNUsort, we first wrote a sort of our
own. Our sort was complementary to GNU's sort; we fixed the problems
they had and ignored the bits they did well -- with the intention of
eventually marrying the two programs and producing an offspring which did
*everything* well! (Specifically, we kept GNU's 'line' structure so that
someday we could adopt their clever key stuff.)

(premature comment below for when I've finished hacking :-) )
This we have now done.  This program is a full-featured sort based on GNUsort,
which we hereby offer to the FSF.  [[At the moment, it is only partially
complete, and still awaits handling of large (> avail mem) files.  However
the algorithms for this have already been implemented in the first draft,
so shouldn't take long to bolt on.]]

Here are the three major problems with GNUsort which we've solved (any others
are minor and incidental):

1) Its use of malloc is *very* dodgy.  Not only does it fragment the heap,
   but it make assumptions about how much memory it is going to need which
   aren't valid.  It frequently runs out of heap space when sorting large
   files, especially when they are mostly composed of short lines.
       [We don't do bitty little mallocs, and we always have enough space]

   (It uses too much ram for things other than text -- we had trouble
    sorting 4Mb files on a 16Mb system.  The actual sort itself which
    needs an extra area the size of the line pointer array is the
    biggest culprit)

   On a virtual memory system, of course, the fragmentation etc probably
   won't be noticed -- but performance will degrade badly as soon as the
   memory in use exceeds the working set size.
       [We know exactly how much memory we are using (necessary for a
        physical memory system) and we never exceed it.]

2) It makes heavy use of temporary files when merging.  This causes two
   problems; Firstly, on a big file, it can generate hundreds of temporary
   files.  On our filing system this is disasterous -- we can only have 76
   files in a directory; Secondly, it has many files open at once.  Our C
   runtime library has a ridiculously small limit on the number of open files
   allowed.
       [We use *one* temporary file, at the expense of sometimes sorting the
        input file in place (ie destructively).]

3) It needs twice the size of the input file to be free on the disk.  (One
   size-of-input for the temporary files, and one for the output file.)
       [We offer a 'sort-in-place' option.  In fact, sort-in-place is the
        primitive default from which the sort-to-output can be manufactured]

What follows is a brief description of our file & store management policy.

         <----------------------- start of physical memory (low addresses)
         [chunk desc    #1]
         [chunk desc    #2]        | These grow this way. One is added for each
         [chunk desc    #3]        | new chunk and all are preserved over chunks.
         [chunk desc   ...]        |
         [chunk desc    #n]       \ /
                                   v
         Space from here down is reused for each chunk.

         [line from file  #1\n]
         [line from file  #2\n]    | Lines of text are added in natural order.
         [line from file ...\n]    | (newlines are converted to NULs)
         [line from file  #n\n]   \ /
                                   v

         [ ... small unused gap ...]
         When the lines and descriptors meet, the chunk is ready to be sorted.

         [line desc   #n]
         [line desc #n-1]         /^\ Fixed-length descriptors (one per line)
         [line desc   #3]          |  are added from the bottom of memory.
         [line desc   #2]          |  It is these which are actually sorted.
         [line desc   #1]          |
         <----------------------- end of physical memory (high addresses)


We sort large files in chunks -- chunks and associated control info are
defined as being however much we can fit into our large single-malloc store
buffer.  Each chunk is sorted in store and the sorted section written out to
file.  We keep a record of where all the chunks are in the file in our
growing array of chunk descriptors. (See above).

[It is axiomatic that sorts go faster when the data is entirely in physical
 storage; this is a situation where virtual memory doesn't help.  This sort
 program gropes with a binary chopping malloc to get all physical memory on a
 physical memory machine; on a VM machine it must be *told* how much store to
 use, and that size must fit in the active working set of pages.]

When building a chunk for sorting, we read in lines sequentially.  Each line
is added to our big buffer in increasing order, and a record describing the
line is added to the end of the buffer in decreasing order.  We stop adding
text just before the two areas cross.  To keep the code simple, we backtrack
the last partial line to the start of that line, so chunks always consist of
whole lines.  [[Not quite true; rather than read line-sized units at a time,
we assume fread() is optimised for big chunks, and get as much as will
possibly fit in the buffer.  If it is a big file, clearly the text at the
end will have to be thrown away as it is overwritten by line pointers]]

(If a single line is too big for our (gigantic) buffer, we put it in a chunk
of its own and deem it already sorted. (we point a chunk descriptor at it
and skip along the input file until we eventually hit the newline).  This
passes the buck to the merge procedure to correctly merge this chunk with
the others. Unfortunately, our merge procedure doesn't cope with it at the
moment either! :-( )

We sort using qsort on the array of line descriptors as a normal sort would.
The comparison routine passed to qsort allows us to sort using all the fancy
options of GNUsort. (We don't actually do so yet as it happens, but we use
the same 'line' format as GNUsort so such options should be easy to add
later).

We write this sorted chunk out to file and continue this process until the
whole file has been sorted.  Since our ram area is empty betwen chunks, we
can easily add another chunk descriptor to our array.

Incidentally, as sorting optimisations we do the following:

1) If the lines were already sorted (actually, the line descriptors being
   in reverse order!) we don't need to sort.  And since we are in fact doing a
   destructive sort of the input file in place, we don't even need to write
   the data back to file.

2) If the lines were already sorted in reverse (ie the line descriptors
   were sorted forwards), we simply reverse all the line descriptors in situ.

3) Otherwise, we need to sort.  Some systems have a sort routine which
   can degrade badly on partially sorted input, so we use the standard
   extremely fast card-shuffling algorithm to randomise the order of the
   line descriptors. This is *not* a significant overhead, and guarantees
   typical-case performance from any sort, rather than risking a bad
   worst-case on some systems.

Having sorted the line descriptors, we loop over them, writing the lines
they point to back out to file.  (We could actually have looped over them
backwards in the case of the already-reverse-sorted chunk, but the overhead
of reversing the actual records was pretty low anyway).


Now having a file which is sorted internally in large chunks, we must merge
these chunks to the output file.  (Of course, if we have only one chunk
left, our job is done.  In fact, we do a quick scan down our list of chunks,
and if we discover that the last line of a chunk is ordered before the first
line of the adjacent chunk, then we can merge those two chunks into a larger
whole.  In this way, a large almost-sorted file can collapse into a single
block and no more work need be done...)

A few moments thought will show that it is impossible (or at least *very*
difficult) to merge chunks of a file back to that file in situ.  However it
is only when we have sorted a file and ended up with disordered chunks that
we need this extra space.  So, we merge the chunks of the file to another
output file, delete the input file, and rename the output file back to the
input.  (If we were sorting from an input file to an output file, we can
skip that bit).


The model of merging that we use is slightly warped.  We pretend that each
chunk in the file is a file in its own right, and we pretend that we are
feeding each of these files into a 'merge' program which takes two files as
input and produces one file as output; furthermore, we pretend that we have
a binary tree of these 'merge' processes feeding one into the other via unix
pipes!

(something like this... [building this balanced tree without recursion or
mallocs was fun :-)])

                                  <---- file 1
                      <---- merge |
                      |           <---- file 2
                      |
                      |
          <---- merge |
                      |           <---- file 3
                      |           |
                      |           |
                      <---- merge |
                                  |           <---- file 4
                                  |           |
                                  <---- merge |
                                              |
                                              <---- file 5


In fact, we do nothing of the sort.  What we actually do is construct a
lazy-evaluation tree of procedure invocations, whereby we get the next line
to be output by calling the root procedure node and asking for a line.  This
request is passed down a calling hierarchy and eventually gets data from a
leaf node out of a chunk of the file.  To avoid dreadful thrashing problems,
each of the <N> leaf nodes has been given 1/N of our original big buffer (no
longer needed for sorting...).  This almost entirely removes any random disk
seeking overhead.  Again, the pipe analogy is not quite valid; what we
actually pass up the tree are pointers to the lines in the cached buffers.
The messages passed down the tree are therefore not "give me a line" but
"make sure the leaf node has a line in store, and pass me a pointer to that
leaf node's line descriptor".  The line itself is consumed at the highest
level by directly tweaking the leaf's line descriptor.  I've a feeling there's
a silly overhead here somewhere and that I should run back up the tree from
the leaf, invalidating those branches, so that I don't need to do a whole
tree compare of all branches to make it valid again.  Must think about it
more when I've sobered up %-}


By this stage, our memory map has changed a little, but we are still using
the technique of allocating from a large fixed buffer.

         <----------------------- start of physical memory (low addresses)
         [chunk desc    #1]
         [chunk desc    #2]        | Chunks were all allocated by end of sort phase.
         [chunk desc    #3]        |
         [chunk desc   ...]        |
         [chunk desc    #n]        -

         [pipe          #1]
         [pipe          #2]        | These grow this way.  A tree of pipes is
         [pipe          #3]        | built up.  This array is finished by the
         [pipe         ...]        | time we start merging.
         [pipe          #n]       \ /
                                   v
                                   -

         Space from here down is equally divided among all chunks for buffering.

         [buffer for chunk  #1\n]

         [buffer for chunk  #2\n]

         [buffer for chunk  #3\n]

         [buffer for chunk  ...\n]

         [buffer for chunk  #n-2\n]

         [buffer for chunk  #n-1\n]

         [buffer for chunk  #n\n]

         [A little left-over space at the end, as all chunk buffers were created
          of exactly the same size.  No real reason for this, just happened that way]

         <----------------------- end of physical memory (high addresses)

                                  We don't need arrays of line decriptors; we
                                  find each line when we need it, and the
                                  details of the current line are held in
                                  the chunk's buffer.


Limitations: Like GNUsort [& AT&T's sort, it transpires...], we're limited to
sorting files whose longest lines fit in store.  If we could hack a comparison
function which worked directly on file (or were using mmap'ed input files) this
wouldn't be a problem.  The longest line during merging is sadly much smaller
than the longest line during sorting.  Whether this will be a problem remains
to be seen.  It may be that we will fail on the merge stage on certain files.

AHA!  One of the problems I'd had with this so far was that of losing text
off the end of the buffer when building up the array of line pointers.  This
forced me to make the design decision that you could only sort from a file,
not a sequential device (like stdin).  The main difficulty I had was where
to store the leftover text while sorting, when waiting for the next run.
I've just realised that the thing to do is to shuffle the text buffer round
so that the leftover text is at the start of the buffer rather than the end --
so you can append to it in the next run. [on reflection, maybe not.  Later, dudes.]

I've always wanted a chance to use this rotation algorithm :-) ...

Assume the numbers are the leftovers...

                                        abcdefghijklmnopqrstuvwyz0123456789
First, reverse all the characters:      \____________ swap _______________/

                                        9876543210zywvutsrqponmlkjihgfedcba
Then reverse the two sections:          \_ swap _/\________ swap _________/

Giving:                                 0123456789abcdefghijklmnopqrstuvwyz

(There may be other ways to do this, but I like this one :-) )

**************************************************************************/

/*
 * Algorithmically, this sort relies on having large contiguous areas of
 * memory.  If you are compiling on a DOS box, make sure you use what is
 * called 'huge model'.  Otherwise most of the sorting performance will be
 * lost through having merge() do the work which sort() should be doing...
 * Though I'm afraid to admit I have used a few practices which might make it
 * harder to port to DOS. Sorry folks.
 */

static char     version[] = "GNew sort, version 0.1.3";
/* Release.version.edit */
#include <stdio.h>
#include <errno.h>
#include <ctype.h>
#include <string.h>

#include <stddef.h>
#include <limits.h>
#include <stdlib.h>
#include <stdarg.h>
#include <time.h>

#ifdef MEMDEBUG
/*
 * Try to explicitly free all memory on exit if you can -- this will help
 * mnemosyne find your memory leaks.
 */
#include <mnemosyn.h>           /* My hack of Marcus J Ranum's *truly
                                 * excellent* debugging package */
#else
#define CHECKHEAP()
#endif

#ifdef FILEDEBUG
#include "filebug.h"            /* for now - later to be <filebug.h> */
#endif

#include "sort.h"

/* Error/debug handling procedures */
int             sperror_debug = (0 != 0);

char           *sperror_debuglog = "";  /* "" signals 'look up DEBUGLOG
                                         * variable'. May be set to NULL to
                                         * save log space */

#ifdef __riscos
#pragma -v1                     /* hint to the compiler to check f/s/printf
                                 * format */
#endif
static void
debugf(const char *fmt,...)
{                               /* Debugf does NOT imply a \n on the end of
                                 * its arg */
    static char     outbuf[256];
    static char     tims[32];
    FILE           *logfile;
    va_list         arg_ptr;
    time_t          tim;

    if (!sperror_debuglog)
        return;
    va_start(arg_ptr, fmt);
    vsprintf(outbuf, fmt, arg_ptr);
    va_end(arg_ptr);
    time(&tim);
    sprintf(tims, "%s", ctime(&tim));
    tims[strlen(tims) - 1] = '\0';
    fprintf(stderr, "%s %s", tims, outbuf);
    if (sperror_debuglog == NULL)
        return;                 /* Logging suppressed by programmer */
    if (*sperror_debuglog == '\0')
        sperror_debuglog = getenv("DEBUGLOG");
#ifdef __riscos
    if (sperror_debuglog == NULL)
        sperror_debuglog = "scsi:$.informat.dx2.sperrorlog";
#else
    if (sperror_debuglog == NULL)
        sperror_debuglog = "sperror.log";
#endif
    logfile = fopen(sperror_debuglog, "a");
    if (logfile != NULL) {
        fprintf(logfile, "%s", outbuf);
        fclose(logfile);
    }
}

#ifdef __riscos
#pragma -v1                     /* hint to the compiler to check f/s/printf
                                 * format */
#endif
static void
sperror(int line, char *file, const char *fmt,...)
{                               /* sperror implies a \n after its arg */
    static char     outbuf[256], s[256];
    FILE           *logfile;
    va_list         arg_ptr;

    va_start(arg_ptr, fmt);
    vsprintf(outbuf, fmt, arg_ptr);
    va_end(arg_ptr);
    if (sperror_debug) {
        sprintf(s, "%s, line %d: %s\n", file, line, outbuf);
    } else {
        sprintf(s, "%s\n", outbuf);
    }
    fprintf(stderr, "%s", s);
    if (sperror_debuglog == NULL)
        return;                 /* Logging suppressed by programmer */
    if (*sperror_debuglog == '\0')
        sperror_debuglog = getenv("DEBUGLOG");
    if (sperror_debuglog == NULL)
        sperror_debuglog = "scsi:$.informat.dx2.sperrorlog";
    logfile = fopen(sperror_debuglog, "a");
    if (logfile != NULL) {
        fprintf(logfile, "%s", s);
        fclose(logfile);
    }
}

/* Initialize the character class tables. */
static void
inittables(void)
{
    int             i;

    /*
     * Note - this could benefit from judicious use of setlocale() --
     * although it is reasonably 8-bit-clean, the tolower() etc calls below
     * unfortunately impose 7-bit character sets by default. (Best of all
     * would be if the locale could be passed in as an optional parameter, or
     * via an environmental variable.)
     */
    for (i = 0; i < (UCHAR_MAX + 1); ++i) {
        if (isspace(i))
            blanks[i] = 1;
        if (isdigit(i))
            digits[i] = 1;
        if (!isprint(i))
            nonprinting[i] = 1;
        if (!isalnum(i) && !isspace(i))
            nondictionary[i] = 1;
        if (isupper(i))
            fold_tolower[i] = tolower(i);
        else
            fold_tolower[i] = i;
    }
}

/*
 * Useful little (!?) debug utility to be inserted at dubious points in the
 * code when mucking about with anything which changes the shape of the large
 * buffer.
 */
static void
display_mem_usage(char *where)
{
    int             i;
    /*********************************************************************

    sort phase:
               [pipechunk   0]
               [pipechunk ...]
               [pipechunk n-1]
               [text     ]
               [  .free. ]
               [line  n-1]
               [line  ...]
               [line    0]

    merge phase:
               [pipechunk   0]
               [pipechunk ...]
               [pipechunk n-1]
               [pipe    0]
               [pipe  ...]
               [pipe  m-1]

               [text    0]
               [text  ...]
               [text  n-1]
               [  .free. ]

    *********************************************************************/

    debugf("---------------------------------------- %s\n", where);
    debugf("&x->pipe[x->next_free_pipe = %d] = %p\n",
           x->next_free_pipe, &x->pipe[x->next_free_pipe]);
    debugf("x->cur_chunk->text = %p\n", x->cur_chunk->text);
    debugf("x->cur_chunk->text_accepted = %p\n", x->cur_chunk->text_accepted);
    debugf("x->cur_chunk->text_lim = %p\n", x->cur_chunk->text_lim);
    debugf("x->buffer_lim = %p\n", x->buffer_lim);
    debugf("&x->line[x->next_used_line = %d] = %p\n",   /* Add limit of line's
                                                         * text? */
           x->next_used_line, &x->line[x->next_used_line]);
    debugf("&x->line[x->line_base = %d] = %p\n",
           x->line_base, &x->line[x->line_base]);

    if (x->next_free_pipe == 0) {
        debugf("%p   [--no pipes--]\n", &x->pipe[0]);
    } else {
        for (i = 0; i < x->next_free_pipe; i++) {
            debugf("%p   [pipe %d]   %p\n", &x->pipe[i], i, &x->pipe[i + 1]);
        }
    }

    debugf("%p   [text lines... (%d bytes) -> ]   %p\n",
           x->cur_chunk->text, x->cur_chunk->text_accepted - x->cur_chunk->text, x->cur_chunk->text_accepted);

    debugf("%p   [raw text... (%d bytes) -> ]   %p\n",
           x->cur_chunk->text_accepted, x->cur_chunk->text_lim - x->cur_chunk->text_accepted, x->cur_chunk->text_lim);

    if ((x->buffer_lim - x->cur_chunk->text_lim) < 1024) {
        debugf("%p   [--small gap--]   %p\n", x->cur_chunk->text_lim, x->buffer_lim);
    } else {
        debugf("%p   [--large gap--]   %p\n", x->cur_chunk->text_lim, x->buffer_lim);
    }

    if (x->next_used_line == x->line_base) {
        debugf("           [--no lines--]   %p\n", &x->line[x->line_base]);
    } else {
        if (x->line_base - x->next_used_line > 1) {
            /* First line */
            debugf("%p   [line  0 (%d) -> %p \"%s\"]\n",
                   &x->line[x->next_used_line], x->next_used_line,
                   x->line[x->next_used_line + 1].text,
                   x->line[x->next_used_line + 1].text);
        }
        /* Last or only line */
        debugf("%p   [line  %d (%d) -> %p \"%s\"]   %p\n",
               &x->line[x->line_base - 1],
               x->line_base - 1 - x->next_used_line, x->line_base - 1,
             x->line[x->line_base - 1].text, x->line[x->line_base - 1].text,
               &x->line[x->line_base]);
    }

    debugf("%p   [--end of memory--]\n", &x->line[x->line_base]);
}


/*
 * This isn't a general-purpose allocator: we rely on it returning objects
 * sequentially.
 */

static PIPE    *
new_pipe(void)
{
    PIPE           *p;

    p = &x->pipe[x->next_free_pipe];
    memset(p, '\0', sizeof(PIPE));
    x->next_free_pipe += 1;
    return (p);
}

/*
 * Get another chunk descriptor.  Only called when big buffer is empty.
 */

PIPE           *
make_pipechunk(void)
{
    PIPE           *p;
    p = new_pipe();
    p->type = DATA;
    return (p);
}

/*
 * Get another pipe operator descriptor.  Only called after all chunks have
 * been sorted, and before merging.
 */

PIPE           *
make_pipeoper(void)
{
    PIPE           *p;
    p = new_pipe();
    p->type = MERGE;
    return (p);
}

static PIPE    *
merge_pipes(PIPE * p, PIPE * left, PIPE * right)
{
    p->node.operand.left = left;
    p->node.operand.right = right;
    return (p);
}

static void
display_chunk(CHUNK * c, char *why)
{
    debugf("chunk @ %p:                --- %s\n", c, why);
    debugf("c->buf = %p\n", c->buf);
    debugf("c->buf_lim = %p\n", c->buf_lim);
    debugf("c->line = %p\n", &c->line); /* Dunno whether to delve into this */
    debugf("c->stored_chunk_filename = %s\n", (c->stored_chunk_filename == NULL ? "(null)" : c->stored_chunk_filename));
    debugf("c->stored_chunk_file = %p\n", c->stored_chunk_file);
    debugf("c->file_start_pos = %ld\n", c->file_start_pos);
    debugf("c->file_chunk_length = %ld\n", c->file_chunk_length);
    debugf("c->text = %p\n", c->text);
    debugf("c->text_accepted = %p\n", c->text_accepted);
    debugf("c->text_lim = %p\n", c->text_lim);
}

static void
replenish_chunk(CHUNK * chunk)
{
    size_t          bytes_got, valid_length, relocation_offset, free_space;
    if (chunk->file_chunk_length == 0) {
        /* debugf("Chunk done.\n"); */
        return;
    }
    /*
     * (this shouldn't be called with a valid line up the spout, but we'll
     * assume it can since we might want to call it in this way at some point
     * in the future.)
     */
    /* Move any text at chunk->text back up to chunk->buf */
    valid_length = chunk->text_lim - chunk->text;
    /* display_chunk(chunk, "before replenishment"); */
    /* debugf("on entry to replenish, text_lim is %p\n", chunk->text_lim); */
    /*
     * debugf("text is %p  buf is %p  text_accepted is %p\n", chunk->text,
     * chunk->buf, chunk->text_accepted);
     */
    relocation_offset = chunk->text - chunk->buf;
    if ((valid_length != 0) && (relocation_offset != 0)) {
        /*
         * No need to move anything if buffer fully empty, said Tom
         * oxymoronically
         */
        /*
         * debugf("memove(to: %p  from: %p  size: %8x)\n", chunk->buf,
         * chunk->text, valid_length);
         */
        memmove(chunk->buf, chunk->text, valid_length); /* may overlap, but
                                                         * memmove is safe */
    }
    /*
     * relocating chunk->line is so tedious that it's easier to throw it away
     * and always find the line again, I've decided.
     */
    /* set chunk->text_lim */
    /*
     * debugf("subtracting %8x from text lim at %p giving %p\n",
     * relocation_offset, chunk->text_lim,
     * chunk->text_lim-relocation_offset);
     */
    chunk->text_lim -= relocation_offset;
    /* Read as much as possible to text_lim short of buf_lim */
    free_space = chunk->buf_lim - chunk->text_lim;
    if (free_space == 0) {
        /* debugf("chunk is full already\n"); */
        return;
    }
    if (chunk->file_chunk_length < free_space) {
        /* Rest of file will fit. */
        free_space = (size_t) chunk->file_chunk_length;
    }
    if (sort_verbose) debugf("getchunk:\n");
    fseek(chunk->stored_chunk_file, chunk->file_start_pos, SEEK_SET);
    /* debugf("reading: to: %p  size: %8x\n", chunk->text_lim, free_space); */
    bytes_got = fread(chunk->text_lim, 1, free_space, chunk->stored_chunk_file);
    if (sort_verbose) debugf("getchunk: %d bytes\n", bytes_got);
    if (bytes_got == -1) {
        sperror(__LINE__, __FILE__, "sort: failed on reading back chunk from %s",
                chunk->stored_chunk_filename);
    }
    /*
     * debugf("adding %8x to text lim at %p giving %p\n", bytes_got,
     * chunk->text_lim, chunk->text_lim+bytes_got);
     */
    chunk->text_lim += bytes_got;
    /* Decrement start_pos and chunk_length appropriately */
    chunk->file_start_pos += bytes_got;
    chunk->file_chunk_length -= bytes_got;
    /* Caller will find first line if chunk->text_accepted isn't valid */
    chunk->text = chunk->text_accepted = chunk->buf;
    /* display_chunk(chunk, "after replenishment"); */
}

static void
preload_chunk_cache(int next_free_chunk)
{
    unsigned char  *free_ptr;
    size_t          available_space, block_size;
    int             i;
    /*
     * Divide up the large buffer into equal sections, and preload as much as
     * possible of each chunk into its respective buffer.  Initialise the
     * chunks internal variables, such as where to seek to in the file to get
     * more data, and how many bytes remain to be read.  
     */
    available_space = ((char *) &x->line[x->next_used_line]
                  - (char *) &x->pipe[x->next_free_pipe]) / next_free_chunk;
    block_size = available_space / next_free_chunk;
    free_ptr = (unsigned char *) &x->pipe[x->next_free_pipe];
    for (i = 0; i < next_free_chunk; i++) {
        x->pipe[i].node.chunk.chunk.buf = free_ptr;
        x->pipe[i].node.chunk.chunk.text = free_ptr;
        x->pipe[i].node.chunk.chunk.text_accepted = free_ptr;
        x->pipe[i].node.chunk.chunk.text_lim = free_ptr;
        /*
         * NOTE: if the size of the chunk is less than the space for this
         * buffer, could allocate to it only that much space as is needed.
         * However if one chunk is too small probably most of them will be,
         * so I can't see any advantage in saving the Ram space.
         * --- yes I can!  Allocate the chunks in reverse, so the last (smallest)
         * comes first.  Bump up free_ptr by min(actual_size, tot_space/slots_left)
         * and perform the bucket calculation anew for each chunk, so the
         * slot can grow larger each time, using the space leftover space.
         * The goal is to fit the whole file in ram if the file size is
         * less than the buffer size, even if only by a few bytes.
         */
        x->pipe[i].node.chunk.chunk.buf_lim = (free_ptr += available_space);
        replenish_chunk(&x->pipe[i].node.chunk.chunk);
    }
    /* debugf("all chunks preloaded\n"); */
}

/* Build a balanced tree bottom-up from all the chunks of the file. */
static PIPE    *
merge_chunks(void)
{
    int             first_pipe, this_pipe, last_pipe;

    /*
     * Note: one optimisation I've been thinking about but haven't done yet
     * -- I should check the consecutive chunks for ordering -- if the last
     * line of one chunk is ordered before the first line of the next chunk,
     * then the two chunks can be concatenated back into one chunk.  Assuming
     * of course that they are stored sequentially in the same chunk file...
     */

    /*
     * One of the problems with GNU sort was that it tried to open as many
     * files as possible at once.  This is a very difficult thing to do, as
     * the number isn't knowable at compile time.  In my first hack with GNU
     * sort, I made that parameter dynamic, by opening lots of files till one
     * failed, deleting them all, and reducing the number I found by a few
     * for emergencies. However, this sort attempts to keep the number of
     * open files down to a minimum.  Most of the chunks to be sorted will be
     * held within the one chunk-file anyway, but even if there are chunks in
     * multiple files to be merged, I'd rather do it by opening the file
     * every time I need a bufferful, and closing it immediately afterwards.
     * I'm assuming all chunk files are seekable. (Which they will be).  The
     * chunks will be large enough (many 10's of Ks; pref many 100's of Ks or
     * even Megs) that the overhead of fopen+fseek+fclose will be minimal
     * relative to reading the data.  [We took this approach in a database
     * project recently -- worked a treat.  In that project we built a fake
     * stdio layer which did all the opening & closing transparently, using
     * an LRU to cache <n> open files out of <m> virtually open files]
     */

    last_pipe = x->next_free_pipe;
    this_pipe = first_pipe = 0;
    /*
     * Then build the pipes into trees.  Sets last_pipe to last of root chunk
     * pipes
     */
    for (;;) {
        /* Built a tree with all pipes merging into one...? */
        if (first_pipe + 1 == last_pipe) {
            /*
             * debugf("We have the final pipe!  first_pipe = %d  last_pipe =
             * %d\n", first_pipe, last_pipe);
             */
            break;
        }
        for (;;) {
            PIPE           *p;
            /*
             * debugf("this_pipe = %d  last_pipe = %d\n", this_pipe,
             * last_pipe);
             */
            if (this_pipe + 1 == last_pipe) {
                /*
                 * debugf("odd number of pipes -- %d gets a bye\n",
                 * this_pipe);
                 */
                break;
            } else if (this_pipe + 1 > last_pipe) {
                break;
            }
            /*
             * debugf("Merging pipes %d and %d -> %d\n", this_pipe, this_pipe
             * + 1, x->next_free_pipe);
             */
            p = make_pipeoper();
            merge_pipes(p, &x->pipe[this_pipe], &x->pipe[this_pipe + 1]);
            this_pipe += 2;
        }
        if (this_pipe + 1 >= last_pipe) {
            /* odd no. of pipes - so give this one a 'bye' to the next level */
            last_pipe = this_pipe;
        }
        this_pipe = first_pipe = last_pipe;
        last_pipe = x->next_free_pipe;
    }
    /*
     * The text buffers for pipe[0..x->next_free_pipe-1] are initialised in
     * preload_chunk_cache()
     */
    /*
     * Now we've done all the mallocking we're going to do, use all that is
     * left over...
     */
    /* Divvie-up the remaining space as buffers for each chunk */
    /* suck the data out of this_pipe ... */
    /* debugf("Root pipe is %d\n", first_pipe); */
    return (&x->pipe[first_pipe]);
}

static int
preload_line(CHUNK * c)
{
    unsigned char  *ptr, *start_of_this_line;
    struct keyfield *key;
    /*
     * Update c->text and c->text_accepted from data in chunk. If can't find
     * a whole line, replenish that chunk first.
     */
    if ((c->text == c->text_lim) && (c->file_chunk_length == 0)) {
        /* End of chunk. All done. Rely on comparison to return the other guy */
        /* debugf("sorry - all gone\n"); */
        return (0 != 0);
    }
    if (c->text == c->text_accepted) {
        /* debugf("getting a line from chunk %p\n", c); */
        if ((ptr = memchr(c->text, '\n', c->text_lim - c->text)) == NULL) {
            /* Chunk needs replenishment */
            replenish_chunk(c);
            if ((ptr = memchr(c->text, '\n', c->text_lim - c->text)) == NULL) {
                sperror(__LINE__, __FILE__, "probably end of chunk?");
                display_chunk(c, "couldn't find \\n even after replenishment");
                exit(EXIT_FAILURE);
            }
        }
        /* This code pinched from findlines() */
        *ptr++ = '\0';
        c->text_accepted = ptr;
        /* debugf("got line %s\n", c->text); */
        /* Oops - almost forgot to findline() on it... */
        start_of_this_line = c->line.text = c->text;
        c->line.length = c->text_accepted - c->text - 1;        /* -1 for lost \n */
        /* Precompute the position of the first key for efficiency. */
        key = keyhead.next;
        if (key != NULL) {
            if (key->eword >= 0) {
                c->line.keylim = limfield(&x->line[x->next_used_line], key);
            } else {
                c->line.keylim = c->text_accepted - 1;
            }
            if (key->sword >= 0) {
                c->line.keybeg = begfield(&c->line, key);
            } else {
                if (key->skipsblanks) {
                    while (blanks[UCHAR(*start_of_this_line)])
                        ++start_of_this_line;
                }
                c->line.keybeg = start_of_this_line;
            }
        }
    } else {
        /* debugf("already have a line: %s\n", c->text); */
    }
    return (0 == 0);
}

int
preload_next_line(PIPE * p, CHUNK ** chunkp, int (*compare) (const void *l, const void *r))
{
#define c (*chunkp)
    CHUNK          *left, *right;
    int             rc;
    if (p->type == MERGE) {
        preload_next_line(p->node.operand.left, &left, compare);
        preload_next_line(p->node.operand.right, &right, compare);
        /* Compare, and set chunk to the first one */
        /* The NULL tests below should be enough but I'm paranoid. */
        if ((left == NULL) || (left->text == left->text_accepted)) {
            /* Left side all done */
            /* debugf("left done. using up right.\n"); */
            if ((right == NULL) || (right->text == right->text_accepted)) {
                /* Both pipes empty.  So we're empty too. */
                c = NULL;
                return (0 != 0);
            }
            c = right;
        } else if ((right == NULL) || (right->text == right->text_accepted)) {
            /* Right side all done */
            /* debugf("right done. using up left.\n"); */
            c = left;
        } else {
            /* both sides valid */
            /*
             * fprintf(stderr, "Comparing %s and %s\n", left->line.text,
             * right->line.text);
             */
            if ((rc = compare(&left->line, &right->line)) < 0) {  // WOOT WOOT!  An old bug finally fixed!
                c = left;
            } else if (rc > 0) {
                c = right;
            } else {
                /* They were the same. Throw one away if sorting '-u' */
                c = left;
            }
        }
    } else if (p->type == DATA) {
        c = &p->node.chunk.chunk;
        if (!preload_line(c)) {
            c = NULL;           /* I suppose. See what happens. */
        }
        /* Set chunk to this one */
    } else {
        sperror(__LINE__, __FILE__, "internal error");
    }
    return (0 == 0);
#undef c
}

static void
consume_pipes(PIPE * root, FILE * outf)
{
    long            outsize;
    long            written;
    CHUNK          *chunk;
    /*
     * Having built up a tree of blocks to be merged, we remove the next line
     * from the head of the tree and write it out.  The PIPE structure passes
     * all the requests down the appropriate branches...
     */
    /* Get line through tree of pipes */
    if (sort_verbose) debugf("merging:\n");
    while (preload_next_line(root, &chunk, compare)) {
        /* Write to output file */
        /* fwrite(chunk->line.text, 1, chunk->line.length, stdout); */
        /* fputc('\n', stdout); */
        if (
          ((written = fwrite(chunk->line.text, 1, chunk->line.length, outf))
           != chunk->line.length)
               ||
               (fputc('\n', outf) == EOF)
            ) {
            sperror(__LINE__, __FILE__,
                 "Disk full on output? - tried to write %d - managed %ld\n",
                    chunk->line.length, written);
            exit(EXIT_FAILURE);
        }
        /* Consume the line */
        chunk->text = chunk->text_accepted;
    }
    if (sort_verbose) debugf("merged:\n");
    /* Now check that size of output file is what we expected! */
    outsize = ftell(outf);
    if (sort_verbose)
        debugf("size of file created was: %ld\n", outsize);
    if (outsize != x->file_length) {
        if (sort_verbose)
            debugf("size of input file was: %ld\n", x->file_length);
        sperror(__LINE__, __FILE__, "sort: internal error");
        exit(EXIT_FAILURE);
    }
}

/* Return a pointer to the first character of a field. */
static unsigned char *
begfield(const LINE * line, struct keyfield * key)
{                               /* MH */
    unsigned char  *ptr = line->text, *lim = ptr + line->length;
    int             sword = key->sword, schar = key->schar;

    if (tab) {
        while (ptr < lim && sword--) {
            while (ptr < lim && *ptr != tab)
                ++ptr;
            if (ptr < lim)
                ++ptr;
        }
    } else {
        while (ptr < lim && sword--) {
            while (ptr < lim && blanks[UCHAR(*ptr)])
                ++ptr;
            while (ptr < lim && !blanks[UCHAR(*ptr)])
                ++ptr;
        }
    }

    if (key->skipsblanks) {
        while (ptr < lim && blanks[UCHAR(*ptr)])
            ++ptr;
    }
    while (ptr < lim && schar--)
        ++ptr;

    return ptr;
}

/*
 * Find the limit of a field; i.e., a pointer to the first character after
 * the field.
 */
static unsigned char *
limfield(const LINE * line, struct keyfield * key)
{                               /* MH */
    unsigned char  *ptr = line->text, *lim = ptr + line->length;
    int             eword = key->eword, echar = key->echar;

    if (tab) {
        while (ptr < lim && eword--) {
            while (ptr < lim && *ptr != tab)
                ++ptr;
            if (ptr < lim && (eword || key->skipeblanks))
                ++ptr;
        }
    } else {
        while (ptr < lim && eword--) {
            while (ptr < lim && blanks[UCHAR(*ptr)])
                ++ptr;
            while (ptr < lim && !blanks[UCHAR(*ptr)])
                ++ptr;
        }
    }

    if (key->skipeblanks) {
        while (ptr < lim && blanks[UCHAR(*ptr)])
            ++ptr;
    }
    while (ptr < lim && echar--)
        ++ptr;

    return ptr;
}

static int
trim_newline()
{
    unsigned char  *on_entry = x->cur_chunk->text_lim;
    /* x->cur_chunk->text_lim is an exclusive pointer */
    for (;;) {
        if (x->cur_chunk->text_lim == x->cur_chunk->text_accepted) {
            x->cur_chunk->text_lim = on_entry;
            return (0 != 0);    /* Restore old x->cur_chunk->text_lim. No
                                 * more lines to find. */
        }
        x->cur_chunk->text_lim = x->cur_chunk->text_lim - 1;
        if (*(x->cur_chunk->text_lim) == '\n')
            break;
    }
    x->cur_chunk->text_lim += 1;/* Now points _after_ newline */
    return (0 == 0);
}

static void
debug_crosscheck(char *s)
{
    if ((long) x->cur_chunk->text_accepted > (long) x->cur_chunk->text_lim) {   /* Assertion check */
        /*
         * We allow the text to fit & exactly fill the buffer on entry before
         * any lines have been allocated
         */
        sperror(__LINE__, __FILE__,
                "buffer pointers have crossed unexpectedly! %s", s);
        sperror(__LINE__, __FILE__,
                "%p > %p ???\n", x->cur_chunk->text_accepted, x->cur_chunk->text_lim);
        display_mem_usage("after crossover");
        exit(EXIT_FAILURE);
    }
}

/*
 * Find the lines in BUF, storing pointers and lengths in LINES. Also replace
 * newlines with NULs.
 */
static void
findlines(void)
{

    /*
     * We fill as much of our buffer as possible with text.  This might fill
     * the entire buffer.  We search for lines from the start of the buffer
     * onwards, and place the pointers to those lines at the end of the
     * buffer backwards.  If the buffer is very full of text, this means that
     * the last umpteen lines will always be lost and overwritten with
     * pointers.  We stop if taking the next line or using one more pointer
     * would cause the two back-to-back stacks to overlap.
     * 
     * When this happens, we trim back the buffer to the end of the last line so
     * that we only have whole lines in the buffer.
     * 
     * GNU's sort takes care to save these odd bits of text, whereas we throw
     * them away willy nilly.  GNU are assuming you could be reading from
     * stdin;  I am not.  I could alter the code to do it the GNU way (with a
     * lot of data shuffling, or maybe temp files[#]) but that will have to
     * wait until everything else is well implemented first. [#]:  Just for
     * the overspill, not for the entire input file.
     * 
     * The code below is still under deconstruction.
     * 
     * Note that if the sort were a simple alpha sort, we could get away with
     * 4-byte pointers (or 8-bytes pointer + length if we're fussy about
     * NULs) rather than the current 16-byte line descriptors. We would have
     * to remember of course to adjust every occurrence of sizeof(LINE), and
     * use a different compare function.  (Why? - to squeeze more lines into
     * Ram of course!)
     * 
     */


    /**************************************************************************

              The pointers into the text are:


                             +--- first value of ptr
                             |
                             v
   buf->text ---> +--------------+   ^
                  |first line\nse|   |
                  |cond line\n...|   |
                  |...           |   |
                  |              |   |
                  |              |   |
                         .           |
                         .           +--- buf->used
                         .           |
                  |              |   |
                  |              |   |
                  |              |   |
                  |              |   |
                  |       ...\nla|   |
                  |st line\nextra|   v
                  +--------------+
      last value of ptr -> ^     ^ <--- Last line is stripped back to previous newline
                           |<-+->|
                              |  |
                 buf->left ---+  +--- buf->text+buf->used


    ***************************************************************************/

    unsigned char  *start_of_this_line = NULL;
    unsigned char  *end_of_line_being_looked_at = NULL;
    struct keyfield *key;

    if (x->cur_chunk->text != x->cur_chunk->text_accepted) {
        sperror(__LINE__, __FILE__, "Must be changing something???");
        exit(EXIT_FAILURE);
    }
    start_of_this_line = x->cur_chunk->text_accepted;   /* Was
                                                         * x->cur_chunk->text,
                                                         * but not strictly best
                                                         * choice */
    key = keyhead.next;

    /*
     * Note: in the code below, x->next_used_line *must* be decremented
     * before the element it is indexing is used, as the initial element is
     * past the end of the heap.  Either that or all operations are on
     * [x->next_used_line-1] and it is decremented afterwards.
     */

    /* Note assertion: buffer already trimmed back to last '\n' */
    /* *should* already be true, but lets be paranoid... */
    if ((long) x->cur_chunk->text_lim <= (long) x->cur_chunk->text) {
        display_mem_usage("buggery :-(");
    }
    if (*(x->cur_chunk->text_lim - 1) != '\n') {
        sperror(__LINE__, __FILE__, "WARNING: buffer was not trimmed.  Why not?\n");
        if (!trim_newline()) {
            /*
             * trim_newline fails when the only text in the buffer is a
             * single partial line with no '\n's anywhere.  If this happens
             * we're in trouble.
             */
            sperror(__LINE__, __FILE__, "sort: Duh, wot???\n");
            exit(EXIT_FAILURE);
        }
    }
    debug_crosscheck("even before we start! - haven't trimmed?");
    for (;;) {
        debug_crosscheck("top of loop");
        if (start_of_this_line >= x->cur_chunk->text_lim) {
            sperror(__LINE__, __FILE__, "*** Justified paranoia... ?\n");
            break;
        }
        /*
         * but search_limit must also be reduced by the descending stack of
         * line descriptors.
         */
        x->next_used_line -= 1; /* Allocate another line element */
        x->buffer_lim = (unsigned char *) &x->line[x->next_used_line];

        if ((long) x->cur_chunk->text_accepted >= (long) x->buffer_lim) {
            /* Oops -- too far. Undo and exit */
            x->cur_chunk->text_lim = x->cur_chunk->text_accepted;       /* Tidy up a little. */
            x->next_used_line += 1;     /* Last one was OK */
            x->buffer_lim = (unsigned char *) &x->line[x->next_used_line];
            break;
        }
        memset(&x->line[x->next_used_line], '\0', sizeof(LINE));
        if ((long) x->cur_chunk->text_lim > (long) x->buffer_lim) {
            x->cur_chunk->text_lim = x->buffer_lim;     /* TRIM NEEDED */
            /*
             * We allocated another line element, and in so doing backtracked
             * over the end of a line.
             */
            /*
             * Trim the buffer back to the last '\n'.  When we do this, we
             * *must* update *all* the significant variables in the global
             * record -- especially the file pointer, so that when we read
             * the next block, we seek to the correct start address.  This
             * turns out to the be the reason that we open the file in "b"
             * mode (you *were* wondering, huh?) -- so that arithmetic on
             * file offsets works correctly.  (Bit of a problem here for
             * folks with \r\n pairs :-( )
             */
            /*
             * Major bug uncovered: was calling this when text_lim was
             * exactly equal to text_accepted.  Unfortunately, the code in
             * trim_newline decremented one of the pointers before checking
             * the two were equal, so it raced off into the boondocks.  Fixed
             * in trim_newline.
             */
            debug_crosscheck("before trimming");
            if (!trim_newline()) {
                debug_crosscheck("after failed trim");
                /*
                 * What has happened here is that we've just dropped
                 * next_used_line back over the last newline in the file. The
                 * next newline backwards is in fact the end of the last
                 * valid and included line.  Therefore there are no more
                 * lines for us to find.  We should undo this prospective
                 * line item and exit from the loop.
                 */
                /*
                 * caveat: potential problem with one-line file which just
                 * fits but newline comes within area of first (=only) line
                 * descriptor. Bump down line descriptor *before* reading
                 * text?
                 */
                x->next_used_line += 1; /* Won't fit this one in. */
                x->buffer_lim = (unsigned char *) &x->line[x->next_used_line];
                /* debugf("End of text as expected when stacks meet\n"); */
                break;
            }
            debug_crosscheck("after trimming");
        }
        /*
         * There are various places in the code that rely on a NUL being at
         * the end of in-core lines; NULs inside the lines will not cause
         * trouble, though.
         */
        if ((end_of_line_being_looked_at = (unsigned char *)
             memchr(start_of_this_line, '\n', x->cur_chunk->text_lim - x->cur_chunk->text_accepted)) == NULL) {
            sperror(__LINE__, __FILE__, "*** Assertion fails: line was not \\n terminated.");
            display_mem_usage("after bad memchr");
            break;
        }
        *end_of_line_being_looked_at = '\0';    /* Zap the newline with a
                                                 * NUL. */
        x->line[x->next_used_line].text = start_of_this_line;
        x->line[x->next_used_line].length = end_of_line_being_looked_at - start_of_this_line;
        /*
         * debugf("Setting x->line[x->next_used_line = %d].text =
         * start_of_this_line = %p = \"%s\";\n", x->next_used_line,
         * start_of_this_line, start_of_this_line);
         */
        /* Precompute the position of the first key for efficiency. */
        if (key != NULL) {
            if (key->eword >= 0) {
                x->line[x->next_used_line].keylim = limfield(&x->line[x->next_used_line], key);
            } else {
                x->line[x->next_used_line].keylim = end_of_line_being_looked_at;
            }
            if (key->sword >= 0) {
                x->line[x->next_used_line].keybeg = begfield(&x->line[x->next_used_line], key);
            } else {
                if (key->skipsblanks) {
                    while (blanks[UCHAR(*start_of_this_line)])
                        ++start_of_this_line;
                }
                x->line[x->next_used_line].keybeg = start_of_this_line;
            }
        }
        x->cur_chunk->text_accepted = start_of_this_line = end_of_line_being_looked_at + 1;
        if (x->cur_chunk->text_accepted == x->cur_chunk->text_lim) {
            /*
             * This only happens when there is spare space in the buffer --
             * when things are tight, the exit some lines above is more
             * likely...
             */
            /* debugf("End of text as expected when file is small...\n"); */
            break;
        }
    }
    debug_crosscheck("most unexpectedly");
}

/*
 * Compare two strings containing decimal fractions < 1.  Each string should
 * begin with a decimal point followed immediately by the digits of the
 * fraction.  Strings not of this form are considered to be zero.
 */
static int
fraccompare(const unsigned char *a, const unsigned char *b)
{                               /* MH */
    int             tmpa = UCHAR(*a);
    int             tmpb = UCHAR(*b);

    if (tmpa == '.' && tmpb == '.') {
        do {
            tmpa = UCHAR(*++a);
            tmpb = UCHAR(*++b);
        } while (tmpa == tmpb && digits[tmpa]);
        if (digits[tmpa] && digits[tmpb])
            return tmpa - tmpb;
        if (digits[tmpa]) {
            while (tmpa == '0')
                tmpa = UCHAR(*++a);
            if (digits[tmpa])
                return 1;
            return 0;
        }
        if (digits[tmpb]) {
            while (tmpb == '0')
                tmpb = UCHAR(*++b);
            if (digits[tmpb])
                return -1;
            return 0;
        }
        return 0;
    } else if (tmpa == '.') {
        do {
            tmpa = UCHAR(*++a);
        } while (tmpa == '0');
        if (digits[tmpa])
            return 1;
        return 0;
    } else if (tmpb == '.') {
        do {
            tmpb = UCHAR(*++b);
        } while (tmpb == '0');
        if (digits[tmpb])
            return -1;
        return 0;
    }
    return 0;
}

/*
 * Compare two strings as numbers without explicitly converting them to
 * machine numbers.  Comparatively slow for short strings, but asymptotically
 * hideously fast.
 */
static int
numcompare(const unsigned char *a, const unsigned char *b)
{                               /* MH */
    int             tmpa, tmpb, loga, logb, tmp;


    tmpa = UCHAR(*a);
    tmpb = UCHAR(*b);

    if (tmpa == '-') {
        tmpa = UCHAR(*++a);
        if (tmpb != '-') {
            if (digits[tmpa] && digits[tmpb])
                return -1;
            return 0;
        }
        tmpb = UCHAR(*++b);

        while (tmpa == '0')
            tmpa = UCHAR(*++a);
        while (tmpb == '0')
            tmpb = UCHAR(*++b);

        while (tmpa == tmpb && digits[tmpa]) {
            tmpa = UCHAR(*++a);
            tmpb = UCHAR(*++b);
        }

        if (tmpa == '.' && !digits[tmpb] || tmpb == '.' && !digits[tmpa])
            return -fraccompare(a, b);

        if (digits[tmpa]) {
            for (loga = 1; digits[UCHAR(*++a)]; ++loga) /* Loop */
                ;
        } else {
            loga = 0;
        }

        if (digits[tmpb]) {
            for (logb = 1; digits[UCHAR(*++b)]; ++logb) /* Loop */
                ;
        } else {
            logb = 0;
        }

        if ((tmp = logb - loga) != 0)
            return tmp;

        if (!loga)
            return 0;

        return tmpb - tmpa;
    } else if (tmpb == '-') {
        if (digits[UCHAR(tmpa)] && digits[UCHAR(*++b)])
            return 1;
        return 0;
    } else {
        while (tmpa == '0')
            tmpa = UCHAR(*++a);
        while (tmpb == '0')
            tmpb = UCHAR(*++b);

        while (tmpa == tmpb && digits[tmpa]) {
            tmpa = UCHAR(*++a);
            tmpb = UCHAR(*++b);
        }

        if (tmpa == '.' && !digits[tmpb] || tmpb == '.' && !digits[tmpa])
            return fraccompare(a, b);

        if (digits[tmpa]) {
            for (loga = 1; digits[UCHAR(*++a)]; ++loga) /* Loop */
                ;
        } else {
            loga = 0;
        }

        if (digits[tmpb]) {
            for (logb = 1; digits[UCHAR(*++b)]; ++logb) /* Loop */
                ;
        } else {
            logb = 0;
        }

        if ((tmp = loga - logb) != 0)
            return tmp;

        if (!loga)
            return 0;

        return tmpa - tmpb;
    }
}

/*
 * Return an integer <= 12 associated with a month name (0 if the name is not
 * recognized.
 */
static int
getmonth(unsigned char *s, int len)
{                               /* MH */
    char            month[4];
    int             i, lo = 0, hi = 12;

    if (len < 3)
        return 0;

    for (i = 0; i < 3; ++i) {
        month[i] = fold_tolower[UCHAR(s[i])];
    }
    month[3] = '\0';

    while (hi - lo > 1) {
        if (strcmp(month, monthtab[(lo + hi) / 2].name) < 0) {
            hi = (lo + hi) / 2;
        } else {
            lo = (lo + hi) / 2;
        }
    }
    if (strcmp(month, monthtab[lo].name) == 0)
        return monthtab[lo].val;
    return 0;
}

/*
 * Compare two lines trying every key in sequence until there are no more
 * keys or a difference is found.
 */
static int
keycompare(const LINE * a, const LINE * b)
{                               /* MH */
    unsigned char  *texta, *textb, *lima, *limb, *translate;
    int            *ignore;
    struct keyfield *key;
    int             diff = 0, iter = 0, lena, lenb;

    for (key = keyhead.next; key; key = key->next, ++iter) {
        ignore = key->ignore;
        translate = key->translate;

        /* Find the beginning and limit of each field. */
        if (iter) {
            if (key->eword >= 0) {
                lima = limfield(a, key);
                limb = limfield(b, key);
            } else {
                lima = a->text + a->length;
                limb = b->text + b->length;
            }

            if (key->sword >= 0) {
                texta = begfield(a, key);
                textb = begfield(b, key);
            } else {
                texta = a->text, textb = b->text;
                if (key->skipsblanks) {
                    while (texta < lima && blanks[UCHAR(*texta)])
                        ++texta;
                    while (textb < limb && blanks[UCHAR(*textb)])
                        ++textb;
                }
            }
        } else {
            /*
             * For the first iteration only, the key positions have been
             * precomputed for us.
             */
            texta = a->keybeg;
            lima = a->keylim;
            textb = b->keybeg;
            limb = b->keylim;
        }

        /* Find the lengths. */
        lena = lima - texta;
        lenb = limb - textb;
        if (lena < 0)
            lena = 0;
        if (lenb < 0)
            lenb = 0;

        /* Actually compare the fields. */
        if (key->numeric) {
            if (*lima || *limb) {
                unsigned char   savea = *lima, saveb = *limb;

                *lima = *limb = '\0';
                diff = numcompare(texta, textb);
                *lima = savea, *limb = saveb;
            } else {
                diff = numcompare(texta, textb);
            }

            if (diff)
                return key->reverse ? -diff : diff;
            continue;
        } else if (key->month) {
            diff = getmonth(texta, lena) - getmonth(textb, lenb);
            if (diff)
                return key->reverse ? -diff : diff;
            continue;
        } else if (ignore && translate) {
            while (texta < lima && textb < limb) {
                while (texta < lima && ignore[UCHAR(*texta)])
                    ++texta;
                while (textb < limb && ignore[UCHAR(*textb)])
                    ++textb;
                if (texta < lima && textb < limb &&
                 translate[UCHAR(*texta++)] != translate[UCHAR(*textb++)]) {
                    diff = translate[UCHAR(*--texta)] - translate[UCHAR(*--textb)];
                    break;
                }
            }
        } else if (ignore) {
            while (texta < lima && textb < limb) {
                while (texta < lima && ignore[UCHAR(*texta)])
                    ++texta;
                while (textb < limb && ignore[UCHAR(*textb)])
                    ++textb;
                if (texta < lima && textb < limb && *texta++ != *textb++) {
                    diff = *--texta - *--textb;
                    break;
                }
            }
        } else if (translate) {
            while (texta < lima && textb < limb) {
                if (translate[UCHAR(*texta++)] != translate[UCHAR(*textb++)]) {
                    diff = translate[UCHAR(*--texta)] - translate[UCHAR(*--textb)];
                    break;
                }
            }
        } else {
            diff = memcmp(texta, textb, MIN(lena, lenb));
        }

        if (diff)
            return key->reverse ? -diff : diff;
        if ((diff = lena - lenb) != 0)
            return key->reverse ? -diff : diff;
    }

    return 0;
}

/*
 * Compare two lines, returning negative, zero, or positive depending on
 * whether A compares less than, equal to, or greater than B.
 */
/*
 * GT: I have this bit of folklore at the back of my mind that some systems
 * don't like static functions to be passed as parameters to external
 * procedures.  I wish I could remember where I heard this or have someone
 * tell me definitely that I'm wrong...
 */
int
compare(const void *av, const void *bv)
{                               /* MH */
    int             diff, tmpa, tmpb, min;
    LINE           *a;
    LINE           *b;

    /*
     * Could also have an optimised case where it goes straight to memcmp
     * when it is a simple alpha sort with no options.  This is worthwhile
     * both for speed and for space -- the line records could be trimmed
     * short after the pointer & length info, thus allowing more lines in the
     * limited Ram.
     */


    a = (LINE *) av;
    b = (LINE *) bv;

    if (keyhead.next) {
        if ((diff = keycompare(a, b)) != 0)
            return diff;
        if (!unique) {
            tmpa = a->length, tmpb = b->length;
            diff = memcmp(a->text, b->text, MIN(tmpa, tmpb));
            if (!diff)
                diff = tmpa - tmpb;
        }
    } else {
        tmpa = a->length, tmpb = b->length;
        min = MIN(tmpa, tmpb);
        if (min == 0) {
            diff = tmpa - tmpb;
        } else {
            unsigned char  *ap = a->text, *bp = b->text;

            diff = *ap - *bp;
            if (diff == 0) {
                diff = memcmp(ap, bp, min);
                if (diff == 0)
                    diff = tmpa - tmpb;
            }
        }
    }

    return reverse ? -diff : diff;
}

/* Quick check to see if already sorted either forwards or reverse. */
/* Base of array is normalised when this is called */
static int
checklines(LINE * lines, int nlines, int (*compare) (const void *l, const void *r))
{
    int             n;
    int             isforward = (0 == 0);
    int             isreverse = (0 == 0);
    int             comp;

    if (nlines == 1) {
        return (0);             /* Actually, don't care, and should have been
                                 * found elsewhere anyway... */
    }
    /* bit of jiggery-pokery will be needed here for -u sorts */

    for (n = 0; n < nlines - 1; n++) {  /* Need QA check that it gets last
                                         * line */
        comp = compare(&lines[n], &lines[n + 1]);
        if (isforward && comp >= 0) {
            if (comp > 0)
                isreverse = (0 != 0);
        } else if (isreverse && comp <= 0) {
            if (comp < 0)
                isforward = (0 != 0);
        } else {
            /* debugf("Unsorted at element %d\n", n); */
            return (0);
        }
        if ((!isforward) && (!isreverse))
            break;
    }
    if (isforward)
        return (1);
    if (isreverse)
        return (-1);
    return (0);
}


/*
 * Compensate for a poor qsort() by making sure data is in random order, this
 * guaranteeing average-case execution time.  Avoids a possible worst-case
 * time if data is almost but not completely sorted already.  (With some
 * sorts, a mostly-forward-sorted file is bad; with others,
 * mostly-reverse-sorted)
 */
/* Base of array is normalised when this is called */
static void
shuffle(LINE * lines, int nlines)
{
    int             n, randnum;
    LINE            temp;

    if (nlines <= 2)
        return;

    for (n = nlines - 1; n > 1; --n) {
        randnum = ((((rand() & 0x7fff) << 15) ^ (rand() & 0xffff)) & 0x7fffffff) % n;
        /* Note complete lack of trust in rand() */
        if ((randnum < 0) || (randnum >= n)) {
            sperror(__LINE__, __FILE__, "random(%d) returned %d!", n, randnum);
        }
        /* Swap line[n] with any of line[0..n-1] */
        memcpy(&temp, &lines[n], sizeof(LINE));
        memcpy(&lines[n], &lines[randnum], sizeof(LINE));
        memcpy(&lines[randnum], &temp, sizeof(LINE));
    }
}


/*
 * Reverse all the pointers to reverse the order of lines. In fact, this is
 * only called to make the pointers reflect the correctly-ordered lines!
 * (because we generate our pointers in a down-growing stack, so they are
 * reversed by default)
 */
/* Base of array is normalised when this is called */
static void
reverse_lines(LINE * lines, int nlines)
{
    int             lo, hi;
    LINE            temp;

    if (nlines == 1)
        return;

    lo = 0;
    hi = nlines - 1;
    for (;;) {
        if (lo >= hi)
            break;              /* Doesn't matter if we miss the middle one
                                 * :-) */
        memcpy(&temp, &lines[lo], sizeof(LINE));
        memcpy(&lines[lo], &lines[hi], sizeof(LINE));
        memcpy(&lines[hi], &temp, sizeof(LINE));
        lo += 1;
        hi -= 1;
    }
}


/* Insert a key at the end of the list. */
static void
insertkey(struct keyfield * key)
{                               /* MH */
    struct keyfield *k = &keyhead;

    while (k->next != NULL)
        k = k->next;
    k->next = key;
    key->next = NULL;
}


/* Free all the keys. */
void
wipe_keys(void)
{
    struct keyfield *k = keyhead.next;
    struct keyfield *last;

    if (sort_debug)
        debugf("Freeing sort-keys\n");
    while (k != NULL) {
        last = k;
        k = k->next;
        free(last);
    }
    keyhead.next = NULL;
}

/* This procedure is called on exit */

tempobj        *templist = NULL;
/* This is an extern because I think atexit() needs an extern. Not sure. */
void
wipe_temps(void)
{
    tempobj        *t;
    while (templist != NULL) {
        t = templist;
        if (sort_verbose)
            debugf("removing temp file %s\n", templist->fname);
        remove(templist->fname);/* Don't care if it fails. */
        templist = templist->next;
        free(t);
    }
}

/*
 * Return a filename which can be used as a temporary file, and add that name
 * to a list of temporary files.  All temporary files not deleted explicitly
 * by the program will be tidied up automatically on exit
 */

static char    *
tempname(void)
{
    char           *t, *tmp;
    tempobj        *remember;

    /* Perhaps a quick check here that file can be opened? */

    tmp = tmpnam(NULL);
    t = malloc(strlen(tmp) + 1);
    strcpy(t, tmp);
    remember = malloc(sizeof(tempobj));
    remember->fname = t;
    if (templist == NULL) {
#ifndef MEMDEBUG
        atexit(wipe_temps);
#endif
        remember->next = NULL;
        templist = remember;
    } else {
        remember->next = templist->next;
        templist->next = remember;
    }
    return (t);
}


static void
usage(void)
{                               /* MH */
    fprintf(stderr, "usage: sort [ -cmuvxYz ] [ -tc ] [ -y<kmem>[K|M]] [ -o file ]\n");
    fprintf(stderr, "            [ -bdfiMnr ] [ +n [ -m ] . . . ] [ files . . . ]\n");
    fprintf(stderr, "       sort -h   for full help information\n");
    exit(EXIT_FAILURE);
}

static void
badfieldspec(char *s)
{
    fprintf(stderr, "sort: bad field specification %s\n", s);
    exit(EXIT_FAILURE);
}

size_t
freemem(void)
{
    char           *p;
    size_t          l, u, m, n;

    l = 0U;
    u = 0xffffffU;
    m = 0U;

    for (;;) {
        if (m == (n = (l + u) / 2U))
            break;
        p = malloc(m = n);
        if (p == NULL) {
            u = m;
        } else {
            l = m;
            free(p);
        }
    }
    return (m);
}

/*
 * Claim the largest malloc we can manage (leaving a little for emergencies,
 * stdio printf's etc...)
 */
static unsigned char *
largest_malloc(size_t * size)
{
#define SLOP 32*1024            /* Doomed to failure on 16-bit machines ;-) */
    unsigned char  *p;
    size_t          local_size;
    if (kmem == -1) {
        local_size = freemem() - SLOP;  /* Largest available if -y not given */
    } else if (kmem == 0) {
        local_size = DEFAULT_SMALL_MEMORY_SIZE; /* Fixed small size if -y0
                                                 * given */
    } else {
        local_size = kmem;      /* -y nnnK given */
    }
    if (local_size < MIN_MEMORY_SIZE)
        local_size = MIN_MEMORY_SIZE;
    /*
     * Tweak it so we have a nice even array of lines.
     */
    local_size = local_size / sizeof(LINE);
    local_size = local_size * sizeof(LINE);
    if (sort_verbose)
        debugf("memory: %u\n", local_size);
    *size = local_size;
    p = malloc(local_size);
    if (p == NULL) {
        sperror(__LINE__, __FILE__, "sort: not enough ram for %u byte buffer\n", local_size);
        exit(EXIT_FAILURE);
    }
    return (p);
}

/*
 * Merge any number of FILES onto the given OFP. merge will never be given an
 * outfile which is also in its list of infiles, unless someone has pulled a
 * stunt like "sort -z -o ./yyy xxx yyy zzz"
 */
static void
merge(char **files, int nfiles, char *outfile)
{
    if (sort_debug) {
        int             i;
        if (sort_verbose)
            debugf("merge the files below to %s:\n", outfile);
        for (i = 0; i < nfiles; i++) {
            if (sort_verbose)
                debugf("   %s\n", files[i]);
        }
    }
    sperror(__LINE__, __FILE__, "sort: merge procedure not yet implemented");
    exit(EXIT_FAILURE);
}

/****************************************************************

The case analysis below has helped me clarify my thinking on what
is done where.  Unfortunately, it is somewhat more complex than
this in the case that we are sorting > 1 file.  In these cases,
we have the possibility of writing the individually sorted
subfiles out as chunks of a bigger temp file.  But we still
have to merge the temp file to the output file.  Of course, if
the 'can delete all input files' flag is set, we don't need
the temp file.  But then we're being less than optimal if all
the files to be sorted would have fitted in Ram -- why touch
the disk at all.



Case analysis:

0000:
can delete input          0
can sort in place         0
file is too big for mem   0
infile == outfile         0

                               Loop
                                   get from input
                                   sort back to input
                               EndLoop
                               merge to temp
                               delete input
                               rename temp to input

can't delete input        1    infile == outfile so can delete input: goto 0000
can sort in place         0
file is too big for mem   0
infile == outfile         0

can delete input          0
can't sort in place       1    infile == outfile so can sort in place: goto 0000
file is too big for mem   0
infile == outfile         0

can't delete input        1    infile == outfile, so can delete input.
can't sort in place       1    infile == outfile, so can sort in place.  goto 0000
file is too big for mem   0
infile == outfile         0

0010:
can delete input          0    Irrelevant when infile == outfile
can sort in place         0
file fits in mem          1
infile == outfile         0

                               get from input
                               sort back to input

can't delete input        1    can sort in place, and infile == outfile, so goto 0010
can sort in place         0
file fits in mem          1
infile == outfile         0

can delete input          0
can't sort in place       1    infile == outfile, so can sort in place: goto 0010
file fits in mem          1
infile == outfile         0

can't delete input        1    Irrelevant when infile == outfile
can't sort in place       1    infile == outfile, so can sort in place: goto 0010
file fits in mem          1
infile == outfile         0

0001:
can delete input          0
can sort in place         0
file is too big for mem   0
infile is not outfile     1

                               delete outfile
                               Loop
                                   get from input
                                   sort back to input
                               EndLoop
                               merge to outfile
                               delete input

1001:
can't delete input        1
can sort in place         0
file is too big for mem   0
infile is not outfile     1

                               Loop
                                   get from input
                                   sort back to input
                               EndLoop
                               merge to outfile

0101:
can delete input          0    Therefore *can* sort-in-place
can't sort in place       1
file is too big for mem   0
infile is not outfile     1

                               Loop
                                   get from input
                                   sort back to input
                               EndLoop
                               merge to outfile
                               delete input

can't delete input        1
can't sort in place       1
file is too big for mem   0
infile is not outfile     1

                               Loop
                                   get from input
                                   sort to temp
                               EndLoop
                               merge temp to outfile
                               delete temp

0011:
can delete input          0
can sort in place         0
file fits in mem          1
infile is not outfile     1

                               delete outfile
                               get from input
                               sort back to input
                               rename input to outfile

can't delete input        1    But sorting in place allowed!
can sort in place         0
file fits in mem          1
infile is not outfile     1

                               delete outfile
                               get from input
                               sort back to outfile

can delete input          0    Therefore *can* sort in place. == 0011
can't sort in place       1
file fits in mem          1
infile is not outfile     1


                               delete outfile
                               get from input
                               sort back to input
                               rename input to outfile


can't delete input        1
can't sort in place       1
file fits in mem          1
infile is not outfile     1

                               delete outfile
                               get from input
                               sort to outfile

Notes:
  Conclusions:

   outfile == infile implies can_sort_in_place
   can_delete_input implies can_sort_in_place
   outfile == infile implies can_delete_input is irrelevant

  Structure:

   if (can_delete_input) can_sort_in_place = TRUE;
   if (outfile == infile) can_sort_in_place = TRUE;

   if (can_sort_in_place) open file in "r+" else in "r".

   Sort first block only in store. Don't write back yet.
   If whole file fitted in store, set file_fits_in_mem.

   Select case as above on can_delete_input.sort_in_place.too_big_for_mem.infile==outfile.

   Jump around cases if equivalent.

   Fits_in_mem cases have sorted data ready.
   Others must write that block back to appropriate location
   and continue sorting.

(an infile which not == an outfile but is in fact the same file
 should be detected by runtime locks on file opening? ("wb"))


*****************************************************************/

static void
writeout(FILE * f, long startoffset)
{
    int             l;
    /* Write the current global buffer data back out to file f */
    /* debugf("writeout(%p SEEK TO %ld)\n", f, startoffset); */
    fseek(f, startoffset, SEEK_SET);
    l = x->next_used_line;
    for (;;) {
        /*
         * -u uniqueness test could go here, although it might be better done
         * at the merge stage (if merging).  I can forsee problems if there
         * is no merging, and we are writing out to the input file: 'trunc()'
         * semantics are not portable. Indeed some systems don't have an
         * equivalent at all :-(
         */
        if (l == x->line_base)
            break;              /* All lines written */
        /* MUST MUST MUST check the return codes of the two lines below!!! */
        if ((fwrite(x->line[l].text, x->line[l].length, 1, f) != 1)
            || (fputc('\n', f) == EOF)) {
            sperror(__LINE__, __FILE__,
                    "sort: error writing data to file -- disk full? write-protected?");
            exit(EXIT_FAILURE);
        }
        l++;                    /* INCREMENT AFTER, YOU DUMMKOPF! -
                                 * NEXT_USED_LINE IS INCLUSIVE!!! */
    }
}

/*
 * Save the data back to the chunk file, *and* note the details of the saved
 * data in the chunk descriptor.
 */
static void
savechunk(CHUNK * c, char *fname, FILE * f, long startoffset, long no_of_bytes)
{
    int             l;
    long            bytes_written = 0;
    /* Write the current global buffer data back out to file f */
    /*
     * debugf("*** writechunk(SEEK TO %ld bytes %ld)\n", startoffset,
     * no_of_bytes);
     */

    fseek(f, startoffset, SEEK_SET);
    c->stored_chunk_filename = fname;
    c->stored_chunk_file = f;
    c->file_start_pos = startoffset;
    c->file_chunk_length = no_of_bytes;
    l = x->next_used_line;
    for (;;) {
        /*
         * -u uniqueness test could go here, although it might be better done
         * at the merge stage (if merging).  I can forsee problems if there
         * is no merging, and we are writing out to the input file: 'trunc()'
         * semantics are not portable. Indeed some systems don't have an
         * equivalent at all :-(
         */
        if (l == x->line_base)
            break;              /* All lines written */
        /* MUST MUST MUST check the return codes of the two lines below!!! */
        if ((fwrite(x->line[l].text, x->line[l].length, 1, f) != 1)
            || (fputc('\n', f) == EOF)) {
            sperror(__LINE__, __FILE__,
                    "sort: error writing data to file -- disk full? write-protected?");
            exit(EXIT_FAILURE);
        }
        bytes_written += (long) x->line[l].length + 1L;
        l++;                    /* INCREMENT AFTER, YOU DUMMKOPF! -
                                 * NEXT_USED_LINE IS INCLUSIVE!!! */
    }
    if (bytes_written != no_of_bytes) {
        /* One explanation: could be added newline. Haven't tweaked that yet. */
        sperror(__LINE__, __FILE__, "*** no_of_bytes (%ld) != bytes_written (%ld)\n", no_of_bytes, bytes_written);
        exit(EXIT_FAILURE);
    }
}

void
readlines(FILE * in)
{
    size_t          got;


    /*
     * See 'caveat' elsewhere: should we bump down first line descriptor in
     * readiness, because of problem with extra-long line whose \n is within
     * area of end line descriptor?
     */
    if (x->cur_chunk->text_lim != x->cur_chunk->text) {
        sperror(__LINE__, __FILE__, "internal error");
        display_mem_usage("text_lim is wrong");
        exit(EXIT_FAILURE);
    }
    for (;;) {                  /* Keep reading while there is space and data
                                 * available */
        /* long            lll; */
        /* debugf("fread -> %p\n", x->cur_chunk->text_lim); */
        /* lll = ftell(in); */
        /*
         * debugf("(ftell says our pointer is %ld -- should be %ld\n", lll,
         * x->file_next_read_pos + (x->cur_chunk->text_lim -
         * x->cur_chunk->text));
         */
        /* display_mem_usage("before fread"); */
        got = fread(x->cur_chunk->text_lim, 1, x->buffer_lim - x->cur_chunk->text_lim, in);
        /*
         * fprintf(stderr, "Contents of buffer at x->cur_chunk->text are:
         * --->");
         */
        /* for (lll = 0; lll < 50; lll++) */
        /* fputc(x->cur_chunk->text[lll], stderr); */
        /* fprintf(stderr, "<---\n"); */
        if (got == -1) {
            sperror(__LINE__, __FILE__, "Unexpected end of file?\n");
            exit(EXIT_FAILURE);
        } else if (got == 0) {
            /* Whole file fits. Not that you're interested */
            /* Check that last line ends in '\n' -- if not, add one */
            if (*(x->cur_chunk->text_lim - 1) != '\n') {
                if (x->cur_chunk->text_lim == x->buffer_lim) {
                    /*
                     * Filled buffer, and last char isn't '\n'.  Better not
                     * add an extra one in that case.  Lets drop through to
                     * trimming code below, and most likely complain & exit
                     */
                } else {
                    sperror(__LINE__, __FILE__,
                    "sort: file did not end in a newline. I've added one.");
                    x->cr_was_missing = (0 == 0);
                    *x->cur_chunk->text_lim = '\n';
                    fprintf(stderr, ">");
                    x->cur_chunk->text_lim += 1;        /* Have already checked
                                                         * there is room */
                    /* Should I note that newline has been added? */
                }
            }
            break;
        } else {
            x->cur_chunk->text_lim += got;
        }
    }

    if (!trim_newline()) {
        sperror(__LINE__, __FILE__, "sort: there appears to be a %d byte line.",
                x->cur_chunk->text_lim - x->cur_chunk->text);
        sperror(__LINE__, __FILE__, "      This should be written out as a single chunk,");
        sperror(__LINE__, __FILE__, "      but I haven't implemented that yet.");
        exit(EXIT_FAILURE);
    }
    /*
     * Having read all the data in doesn't mean that we got the whole file in
     * OK -- if the line-finding overlaps the text, we may have to read some
     * of it again
     */
}

int
perform_sort(int order)
{

    /*
     * It takes a bit of convincing, but the optimisations below still work
     * correctly even when using sort -r.
     */
    /* (Optimisations temporarily removed so I can test sorting!) */
    if ((0 != 0) && order > 0) {
        if (sort_verbose)
            debugf("pointers reverse-sorted, which means data was sorted...\n");
        /*
         * if (in_place) return;
         *//* Might as well reverse pointers anyway... */
        /*
         * Otherwise patch-up by reversing the pointers, and drop through to
         * the same output code as the qsort path...
         */
        if (sort_verbose)
            debugf("reverse:\n");
        reverse_lines(
                         &x->line[x->next_used_line],
                         x->line_base - x->next_used_line);
    } else if ((0 != 0) && order < 0) {
        if (sort_verbose)
            debugf("Data was reversed, but pointers sorted already so no more work to do...\n");
        /* Except writing out via pointers that is... */
    } else {
        /*
         * Original sort prog used its own divide-and-conquer algorithm
         * (basically a partition/merge sort) which is equivalent to
         * quicksort but uses twice as much space.  (It isn't easy to
         * merge-in-place, as we already know :-( ) We call system qsort. If
         * your system has a poor qsort, find a better one. If you can't find
         * a better one, and are having trouble on almost-sorted arrays, try
         * unsorting the data with a perfect shuffle...
         */
        if (sort_verbose)
            debugf("shuffle:\n");       /* What the hell. It hardly costs a
                                         * few percent anyway */
        shuffle(&x->line[x->next_used_line],
                x->line_base - x->next_used_line);
        if (sort_verbose)
            debugf("qsort:\n");
        /*
         * debugf("qsort: %p %d\n", &x->line[x->next_used_line], x->line_base
         * - x->next_used_line);
         */
        /* display_mem_usage("on entering qsort"); */
        if (!unsort)
            qsort(
                     &x->line[x->next_used_line],
                     x->line_base - x->next_used_line,
                     sizeof(LINE),
                     compare);
        /* debugf("qsort done:\n"); */
    }
    return (0 == 0);
}

/*
 * Returns TRUE if more data has to be read, FALSE if the entire file plus
 * line descriptors managed to fit in Ram.  [result val is broken at the
 * moment!]
 */
static int
get_and_sort_block(FILE * in, long startoffset, long file_length, int *optimflag)
{                               /* Guts of sorting */
    int             rc, order;


    /* debugf("get_and_sort: reading - SEEK TO %ld\n", startoffset); */
    fseek(in, startoffset, SEEK_SET);

    /* GLOBAL INITIALISATION: Once-per-chunk initialisations. */
    x->cur_chunk = &(make_pipechunk()->node.chunk.chunk);

    /*
     * -- a quick raw clear of all fields. just to be safe for when we merge
     * --
     */
    x->cur_chunk->buf = NULL;
    x->cur_chunk->buf_lim = NULL;
    x->cur_chunk->line.text = NULL;
    x->cur_chunk->line.length = 0;
    x->cur_chunk->line.keybeg = NULL;
    x->cur_chunk->line.keylim = NULL;

    x->cur_chunk->stored_chunk_filename = NULL;
    x->cur_chunk->stored_chunk_file = NULL;
    x->cur_chunk->file_start_pos = 0L;
    x->cur_chunk->file_chunk_length = 0L;
    x->cur_chunk->text = NULL;
    x->cur_chunk->text_accepted = NULL;
    x->cur_chunk->text_lim = NULL;
    /* -- done -- */

    x->cur_chunk->text = (unsigned char *) &x->pipe[x->next_free_pipe];
    x->cur_chunk->text_lim = x->cur_chunk->text;        /* ptr after last byte
                                                         * read */
    x->cur_chunk->text_accepted = x->cur_chunk->text;   /* ptr after last byte
                                                         * of last line accepted */
    x->buffer_lim = (unsigned char *) &x->line[x->next_used_line = x->line_base];

    /* display_mem_usage("before readlines"); */
    if (sort_verbose)
        debugf("readlines:\n");
    readlines(in);
    if (sort_verbose)
        debugf("findlines:\n");
    /* display_mem_usage("before findlines"); */
    findlines();                /* Updates all the globals flags etc,
                                 * including how much of the file has been
                                 * processed. */

    /* Sort ... */
    /* display_mem_usage("after findlines"); */
    if (sort_verbose)
        debugf("check order:\n");
    order = checklines(&x->line[x->next_used_line], x->line_base - x->next_used_line, compare);

    rc = perform_sort(order);

    if (sort_verbose)
        debugf("done:\n");

    if (sort_verbose)
        debugf("startoffset:  %ld\n", startoffset);
    if (sort_verbose)
        debugf("file length:  %ld\n", file_length);
    if (sort_verbose)
        debugf("bytes sorted: %d\n", (x->cur_chunk->text_accepted - x->cur_chunk->text));
    if (sort_verbose)
        debugf("newline added:%s\n", (x->cr_was_missing ? "yes" : "no"));

    if (startoffset + (x->cur_chunk->text_accepted - x->cur_chunk->text) >= file_length) {
        /* debugf("!No more!\n"); */
        return (0 != 0);        /* No more */
    } else {
        /* debugf("!More!\n"); */
        return (0 == 0);        /* More */
    }
    /* display_mem_usage("after sorting this chunk"); */
}


/*
 * Do *not* exit if there is an error in this routine -- let the caller
 * attempt to recover gracefully. ENTIRELY UNTESTED!!!
 */
static int
copy_back(char *infile, char *outfile)
{
    FILE           *in, *out;
    long            count, written;
    char           *buffer;
    long            max_length;
    buffer = (char *) &x->pipe[x->next_free_pipe];
    max_length = (char *) &x->line[x->line_base] - buffer;
    in = fopen(infile, "rb");
    if (in == NULL) {
        sperror(__LINE__, __FILE__, "sort: cannot reopen input file %s", infile);
        return (0 != 0);
    }
    out = fopen(outfile, "wb");
    if (out == NULL) {
        sperror(__LINE__, __FILE__, "sort: cannot open output file %s", outfile);
        fclose(in);
        return (0 != 0);
    }
    if (sort_verbose) debugf("copy back:\n");
    for (;;) {
        /* Get a big block */
        count = fread(buffer, 1, (size_t) max_length, in);
        if (sort_debug)
            debugf("copy_fread: got %ld\n", count);
        /* Exit if count == EOF */
        if (count == EOF)
            break;
        /* Write a big block if length != 0 */
        if (count != 0) {
            written = fwrite(buffer, 1, (size_t) count, out);
            if (sort_debug)
                debugf("copy_fwrite: put %ld\n", written);
            if (written != count) {
                sperror(__LINE__, __FILE__, "sort: cannot write %ld bytes to file %s",
                        count - written, outfile);
                fclose(in);
                fclose(out);
                return (0 != 0);
            }
        }
    }
    /*
     * Remember to check these one I've found out what they're supposed to
     * return
     */
    fclose(in);
    fclose(out);
    if (sort_verbose) debugf("copy done:\n");
    return (0 == 0);
}

/*
 * Something GNU sort does which I don't yet:  treat an infile of "-" as
 * stdin.  I can't yet handle these properly, though I could bodge it by
 * spooling stdin to a tmp file, and sorting the tmp file in place.
 */

static void
sort(char *infile, char *outfile, int can_sort_in_place, int can_delete_input)
{
    PIPE           *rootpipe;
    int             prev_next_free = 0;
    char           *mergetemp, *savetemp;
#define CHECKFOPEN(f) \
  if (f == NULL) sperror(__LINE__, __FILE__, "sort: file open fails")   /* TEMP */
    int             optimflag, more, rc;
    FILE           *in, *out;


    if (strcmp(infile, outfile) == 0)
        can_sort_in_place = (0 == 0);
    if (can_delete_input)
        can_sort_in_place = (0 == 0);
    if (can_sort_in_place)
        in = fopen(infile, "rb+");
    else
        in = fopen(infile, "rb");
    /*
     * From now on, can test in_place flag to see whether infile was opened
     * in "+" mode or not
     */

    /* GLOBAL INITIALISATION: Per-file once-only initialisations */

    x->next_free_pipe = 0;      /* Should have been anyway... */
    x->file_next_read_pos = 0L;
    x->outfile_next_write_pos = 0L;
    x->cr_was_missing = (0 != 0);
    /* debugf("Seek to 0L/END (note: doesnt seek back)\n"); */
    fseek(in, 0L, SEEK_END);
    x->file_length = ftell(in);
    /* debugf("File length = %ld\n", x->file_length); */

    /* This allocates another chunk descriptor */
    more = get_and_sort_block(in, x->file_next_read_pos, x->file_length, &optimflag);
    if (!more) {
        if (sort_verbose)
            debugf("*** Managed to sort whole file in ram!\n");
        /*
         * Managed to sort all of file in Ram, or this was last block. May
         * even get away with not having to write it back to disk -- depends
         * on optim flag.
         */
        if (strcmp(infile, outfile) == 0) {
            /* Definitely can optimise.  Sort in place is assured. */
            /* Either write-back (rb+ mode) or leave already sorted */
            writeout(in, x->file_next_read_pos);        /* Put it back where we
                                                         * got it */
            return;
        }
        if (can_sort_in_place) {
            /* Delete output file if it exists */
            /* Write back to input file and close it. */
            writeout(in, x->file_next_read_pos);
            /*
             * WARNING! -- on some systems, if a file is write-protected, the
             * data can appear to have been written out OK, and the error
             * will only be flagged when you come to doing the fclose() -- so
             * you must always check fclose return codes...
             */
            fclose(in);
            /* Rename input file to output file */
            rename(infile, outfile);
            /* Should check if it fails, and do a copy + delete instead? */
            return;
        } else {
            fclose(in);
            if (can_delete_input) {
                /* Delete input file */
            }
            /* write to output file */
            out = fopen(outfile, "wb");
            CHECKFOPEN(out);
            writeout(out, 0L);
            return;
        }
        /* NOT REACHED */
        return;
    }
    for (;;) {
        /* Have more blocks to sort -- just go round again */
        /* debugf("sort: more blocks to be sorted...\n"); */

        if (can_sort_in_place) {
            savechunk(x->cur_chunk, infile, in, x->file_next_read_pos,
                      (x->cur_chunk->text_accepted - x->cur_chunk->text));
        } else {
            /* have never tested this branch. */
            savechunk(x->cur_chunk, outfile, out, x->outfile_next_write_pos,
                      (x->cur_chunk->text_accepted - x->cur_chunk->text));
            x->outfile_next_write_pos += (x->cur_chunk->text_accepted - x->cur_chunk->text);
        }

        x->file_next_read_pos += (x->cur_chunk->text_accepted - x->cur_chunk->text);

        if (!more)
            break;              /* That was the last chunk written back. Now
                                 * merge them */

        /* This allocates another chunk descriptor */
        more = get_and_sort_block(in, x->file_next_read_pos, x->file_length, &optimflag);
    }                           /* Keep sorting blocks until file ended */
    prev_next_free = x->next_free_pipe;
    rootpipe = merge_chunks();
    /*
     * pipes 0..prev_next_free-1 are valid chunk descriptors -- those
     * following are merge nodes
     */
    preload_chunk_cache(prev_next_free);
    out = fopen(mergetemp = tempname(), "wb");
    if (out == NULL) {
        sperror(__LINE__, __FILE__, "cannot open temporary file %s for merging", mergetemp);
        exit(EXIT_FAILURE);
    }
    consume_pipes(rootpipe, out);
    fclose(out);
    fclose(in);
    savetemp = tempname();
    rc = rename(infile, savetemp);
    if (rc == 0) {
        rc = rename(mergetemp, outfile);
        if (rc == 0) {
            (void) rename(savetemp, mergetemp); /* Let exit routine delete
                                                 * it! */
            return;
        }
        if (!copy_back(mergetemp, outfile)) {
            /* Really in trouble. Lets try to recover */
            rc = rename(savetemp, infile);
            if (rc == 0) {
            } else {
            }
            return;
        }
        /* savetemp will be removed on exit */
        return;
    }
    /*
     * Couldn't rename input. (Probably another device). So probably won't be
     * able to rename mergetemp either.  Lets go straight to the copy.
     */
    if (!copy_back(mergetemp, outfile)) {
        /*
         * We're in a spot of trouble here. May even have lost the file
         * :-(((((
         */
        sperror(__LINE__, __FILE__, "sort: cannot save output file %s\n", outfile);
        exit(EXIT_FAILURE);
    }
    return;
#undef CHECKFOPEN
}


static void
memsize(char *mems, size_t * kmem)
{
    size_t          i;
    /* Check that mems is either all numeric or numeric+{K|M} */
    if (!isdigit(*mems)) {
        sperror(__LINE__, __FILE__, "-y must be followed by a number, eg 1000000 or 100K or 1M (not %s)", mems);
        exit(EXIT_FAILURE);
    }
    i = 0;
    while (isdigit(*mems)) {
        i = i * 10;
        i = i + ((*mems) - '0');
        mems += 1;
    }
    if (*mems == '\0') {
        *kmem = i;
        return;
    }
    if (*mems == 'K' || *mems == 'k') {
        i = i * 1024;
        *kmem = i;
        return;
    }
    if (*mems == 'M' || *mems == 'm') {
        i = i * 1024 * 1024;
        *kmem = i;
        return;
    }
    sperror(__LINE__, __FILE__, "Unexpected text \"%s\" following -y", mems);
    exit(EXIT_FAILURE);
}


/*
 * This rather LOUD procedure with so many parameters is a first crude cut at
 * reducing the size of the main() routine and making it a bit more
 * hierarchical. (There was nothing wrong with the way it was but splitting
 * it up a bit makes editing a little easier, especially with a folding
 * source editor...) [I'll change the name when I sort out the params a bit
 * better...]
 */
/*
 * Considering adding an extra option to *unsort* a file -- great for
 * testing!
 */
static void
HANDLE_PARAMETERS(
                     int *argcp,
                     char ***argvp,
                     struct keyfield ** keyp,
                     struct keyfield * gkeyp,
                     int *mergeonlyp,
                     int *checkonlyp,
                     char **outfilep,
                     int *nfilesp,
                     char ***filesp,
                     int *ip
)
{                               /* MH */
    /* My usual hack way of simulating Pascal-style VAR parameters */
#define argc (*argcp)
#define argv (*argvp)
#define key (*keyp)
#define gkey (*gkeyp)
#define mergeonly (*mergeonlyp)
#define checkonly (*checkonlyp)
#define outfile (*outfilep)
#define nfiles (*nfilesp)
#define files (*filesp)
#define i (*ip)
    char           *s, *mems;
    int             t, t2;


    gkey.sword = gkey.eword = -1;
    gkey.ignore = NULL;
    gkey.translate = NULL;
    gkey.numeric = gkey.month = gkey.reverse = 0;
    gkey.skipsblanks = gkey.skipeblanks = 0;

    /* For safety, the default for these extensions is 'off' */
    gkey.sort_in_place = (0 != 0);
    gkey.may_corrupt_infiles = (0 != 0);
    gkey.delete_input_files = (0 != 0);

    for (i = 1; i < argc; ++i) {
        if (argv[i][0] == '+') {
            if (keyhead.next == NULL) {
#ifndef MEMDEBUG
                atexit(wipe_keys);      /* Clean up mallocs on exit */
#endif
            }
            if (key) {
                insertkey(key);
            }
            /*
             * *Could* change this malloc below to be one of our private
             * allocators, like new_pipe or new_chunk. Will probably do that
             * later; too busy just now...
             */
            key = (struct keyfield *) malloc(sizeof(struct keyfield));
            key->eword = -1;
            key->ignore = NULL;
            key->translate = NULL;
            key->skipsblanks = key->skipeblanks = 0;
            key->numeric = key->month = key->reverse = 0;
            s = argv[i] + 1;
            /*
             * NOTE: These UCHAR()s are the only ones where it is important
             * -- all the others have unsigned chars as their parameters
             * anyway. These should really be cast as ints and ANDed with 255
             * anyway...
             */
            if (!digits[UCHAR(*s)])
                badfieldspec(argv[i]);
            for (t = 0; digits[UCHAR(*s)]; ++s)
                t = 10 * t + *s - '0';
            t2 = 0;
            if (*s == '.')
                for (++s; digits[UCHAR(*s)]; ++s)
                    t2 = 10 * t2 + *s - '0';
            if (t2 || t) {
                key->sword = t;
                key->schar = t2;
            } else
                key->sword = -1;
            while (*s) {
                switch (*s) {
                case 'b':
                    key->skipsblanks = 1;
                    break;
                case 'd':
                    key->ignore = nondictionary;
                    break;
                case 'f':
                    key->translate = fold_tolower;
                    break;
                case 'i':
                    key->ignore = nonprinting;
                case 'M':
                    key->skipsblanks = key->skipeblanks = key->month = 1;
                    break;
                case 'n':
                    key->skipsblanks = key->skipeblanks = key->numeric = 1;
                    break;
                case 'r':
                    key->reverse = 1;
                    break;
                default:
                    badfieldspec(argv[i]);
                    break;
                }
                ++s;
            }
        } else if (argv[i][0] == '-') {
            if (strcmp("-", argv[i]) == 0)
                break;
            s = argv[i] + 1;
            if (digits[UCHAR(*s)]) {
                if (!key)
                    usage();
                for (t = 0; digits[UCHAR(*s)]; ++s)
                    t = t * 10 + *s - '0';
                t2 = 0;
                if (*s == '.')
                    for (++s; digits[UCHAR(*s)]; ++s)
                        t2 = t2 * 10 + *s - '0';
                key->eword = t;
                key->echar = t2;
                while (*s) {
                    switch (*s) {
                    case 'b':
                        key->skipeblanks = 1;
                        break;
                    case 'd':
                        key->ignore = nondictionary;
                        break;
                    case 'f':
                        key->translate = fold_tolower;
                        break;
                    case 'i':
                        key->ignore = nonprinting;
                    case 'M':
                        key->skipsblanks = key->skipeblanks = key->month = 1;
                        break;
                    case 'n':
                        key->skipsblanks = key->skipeblanks = key->numeric = 1;
                        break;
                    case 'r':
                        key->reverse = 1;
                        break;
                    default:
                        badfieldspec(argv[i]);
                        break;
                    }
                    ++s;
                }
                insertkey(key);
                key = NULL;
            } else
                while (*s) {
                    switch (*s) {
                    case 'b':
                        gkey.skipsblanks = gkey.skipeblanks = 1;
                        break;
                    case 'c':
                        checkonly = 1;
                        break;
                    case 'd':
                        gkey.ignore = nondictionary;
                        break;
                    case 'f':
                        gkey.translate = fold_tolower;
                        break;
                    case 'i':
                        gkey.ignore = nonprinting;
                        break;
                    case 'M':
                        gkey.skipsblanks = gkey.skipeblanks = gkey.month = 1;
                        break;
                    case 'm':
                        mergeonly = 1;
                        break;
                    case 'n':
                        gkey.skipsblanks = gkey.skipeblanks = gkey.numeric = 1;
                        break;
                    case 'o':
                        if (outfile != NULL) {
                            fprintf(stderr, "sort: only one -o or -z allowed\n");
                            exit(EXIT_FAILURE);
                        }
                        if (s[1]) {
                            outfile = s + 1;
                        } else {
                            if (i == argc - 1) {
                                fprintf(stderr, "sort: missing argument to -o");
                                exit(EXIT_FAILURE);
                            } else {
                                outfile = argv[++i];
                            }
                        }
                        goto outer;
                    case 'r':
                        gkey.reverse = reverse = 1;
                        break;
                    case 't':
                        if (s[1]) {
                            tab = *++s;
                        } else if (i < argc - 1) {
                            tab = *argv[++i];
                            goto outer;
                        } else {
                            fprintf(stderr, "sort: missing character for -tc");
                            exit(EXIT_FAILURE);
                        }
                        break;
                    case 'U':
                        unsort = (0 == 0);
                        debugf("sort: WARNING -- -U means 'unsort'\n");
                        break;
                    case 'u':
                        unique = (0 == 0);
                        debugf("sort: WARNING -- -u not yet implemented\n");
                        break;
                    case 'V':
                        sperror(__LINE__, __FILE__, "%s", version);
                        exit(EXIT_SUCCESS);
                        break;

                    case 'v':
                        sort_verbose = (0 == 0);
                        break;

                    case 'x':
                        /*
                         * -x means that the input files should be deleted as
                         * soon as possible
                         */
                        gkey.delete_input_files = (0 == 0);
                        break;

                    case 'y':
                        if (s[1] != '\0') {
                            mems = ++s;
                            s = s + strlen(s) - 1;      /* Points to just before
                                                         * the NUL */
                            memsize(mems, &kmem);
                            break;
                        } else if ((i < argc - 1) && (*argv[i + 1] != '-')) {
                            mems = argv[++i];
                            memsize(mems, &kmem);
                            goto outer;
                        } else if (i < argc - 1) {
                            kmem = -1;  /* -y alone means all available */
                            goto outer;
                        } else {
                            fprintf(stderr, "sort: missing size for -y <memsize>");
                            exit(EXIT_FAILURE);
                        }
                        break;

                    case 'Y':
                        /*
                         * -Y means that the one input file should be sorted
                         * in-place
                         */
                        if (outfile != NULL) {
                            fprintf(stderr, "sort: only one -o or -z allowed\n");
                            exit(EXIT_FAILURE);
                        }
                        gkey.sort_in_place = (0 == 0);
                        gkey.may_corrupt_infiles = (0 == 0);    /* Is implied by
                                                                 * 'sort-in-place' */
                        nfiles = 1;
                        if (s[1] != '\0') {
                            outfile = s + 1;
                        } else {
                            if (i == argc - 1) {
                                fprintf(stderr, "sort: missing argument to -y");
                                exit(EXIT_FAILURE);
                            } else {
                                outfile = argv[++i];
                                files = &argv[i];
                            }
                        }
                        goto outer;

                    case 'z':
                        /*
                         * -z means that any of the input files may be
                         * scrambled; e.g. "sort -z -o fred jim john fred
                         * sheila" is allowed to trample any of jim, john,
                         * fred or sheila.
                         */
                        gkey.may_corrupt_infiles = (0 == 0);
                        break;

                    case 'h':
                        /*
                         * -h prints full help info to stdout. Honest.
                         */
                        printf("GNU SORT by Mike Haertel and Graham Toal, (c) FSF 1988, 1991.\n");
                        printf("\n");
                        printf("    -?      Print short usage info.\n");
                        printf("\n");
                        printf("    -c      Check sort order only.\n");
                        /*
                         * Actually, the test-already-sorted optimisations
                         * are so useful that -m option could be deprecated
                         * (ie sort them anyway since it doesn't cost much)
                         */
                        printf("    -m      Merge presorted files (avoids resorting of already sorted files,\n");
                        printf("            which is generally a slow operation).\n");
                        printf("    -v      Verbose\n");
                        printf("    -y kmem[K|M]  Give <kmem> bytes of phys memory\n");
                        printf("    -ofile  Send output to `file' (overwriting).\n");
                        printf("    -tc     Use c as field separator.\n");
                        printf("    -u      Delete duplicate lines from the output.\n");
                        printf("    -V      Print the version string and return.\n");
                        printf("\n");
                        printf("    These options allow some control over how much disk space is used\n");
                        printf("    (not compatible with Unix sort, and will be removed soon...)\n");
                        printf("\n");
                        printf("    -x      Delete the input file to save disk space. (sort -x -o fred  jim)\n");
                        printf("    -Y file Sort the input file in place. (like sort -o file file)\n");
                        printf("    -z      Allows sort to scramble the input file. (sort -z -o fred  jim john)\n");
                        printf("\n");
                        printf("    Keys are zero based, thus the first field has number 0, and so on.\n");
                        printf("\n");
                        printf("    +num1.num2\n");
                        printf("            Start a new key at character num2 of field num1.\n");
                        printf("    -num1.num2\n");
                        printf("            Extend the key up to (but not including) character num2 of field num1.\n");
                        printf("\n");
                        printf("    These option apply either globally to all sort keys (if they are\n");
                        printf("    specified before the keys on the command line), or to a single key\n");
                        printf("    (if they are appended to the key specification).\n");
                        printf("\n");
                        printf("    -b      Skip white space.\n");
                        printf("    -d      Dictionary mode: ignore punctuation.\n");
                        printf("    -f      Fold to lowercase.\n");
                        printf("    -i      Ignore nonprinting characters.\n");
                        printf("    -M      Sort as english (three character) month names.\n");
                        printf("    -n      Sort numerically.\n");
                        printf("    -r      Sort in reverse.\n");
                        exit(EXIT_SUCCESS);

                    default:
                    case '?':
                        usage();
                        exit(EXIT_FAILURE);
                    }
                    ++s;
                }
        } else
            break;
      outer:;
    }

    if (key) {
        insertkey(key);
    }

    /* Inheritance of global options to individual keys. */
    for (key = keyhead.next; key; key = key->next) {
        if (!key->ignore && !key->translate && !key->skipsblanks && !key->reverse
            && !key->skipeblanks && !key->month && !key->numeric) {
            key->ignore = gkey.ignore;
            key->translate = gkey.translate;
            key->skipsblanks = gkey.skipsblanks;
            key->skipeblanks = gkey.skipeblanks;
            key->month = gkey.month;
            key->numeric = gkey.numeric;
            key->reverse = gkey.reverse;
        }
    }

    if (!keyhead.next && (gkey.ignore || gkey.translate || gkey.skipsblanks
                          || gkey.reverse || gkey.skipeblanks
                          || gkey.month || gkey.numeric)) {
        insertkey(&gkey);
    }
#undef i
#undef files
#undef nfiles
#undef outfile
#undef checkonly
#undef mergeonly
#undef gkey
#undef key
#undef argv
#undef argc
}


/*
 * Not relevant any more, but in the original GNU sort, the author was
 * overwriting past the end of argv. Naughty.  I hope that was just a bug in
 * the MSDOS port -- must remember to pass it on to him when I get in
 * touch...
 */
int
main(int argc, char **argv)
{
    struct keyfield *key = NULL;
    struct keyfield gkey;
    size_t          bigsize;    /* Only used when sizing big buffer */
    int             i;
    int             checkonly = 0, mergeonly = 0, nfiles = 0;
    char           *outfile = NULL, *infile = NULL, *tmp;
    /* Points to array [0..nfiles-1] of char *file, or is NULL for stdin */
    char          **files;
    FILE           *ofp;

    /* Initialisation */

    sperror_debug = (0 == 0);   /* Turn on line numbers in debug info */

    memset(&keyhead, '\0', sizeof(keyhead));    /* Clear keyhead struct. Not really needed. */
    inittables();
    HANDLE_PARAMETERS(
                         &argc, &argv,
                         &key, &gkey,
                         &mergeonly, &checkonly,
                         &outfile, &nfiles, &files, &i);


    /*** Initialise the dynamically allocated buffer. ***/

    /* GLOBAL INITIALISATION: Once-per-run initialisations */
    x = (GLOBAL *) largest_malloc(&bigsize);
    if (x == NULL) {
        sperror(__LINE__, __FILE__, "sort: couldn't claim memory.  damn weird.");
        exit(EXIT_FAILURE);
    }

    /*
     * The lower limit of the (reversed) line array is not element 0, but
     * some point dynamically determined at runtime
     */
    x->line_base = (bigsize / sizeof(LINE));    /* exclusive */
    x->next_used_line = x->line_base;
    x->buffer_lim = (unsigned char *) &x->line[x->next_used_line];      /* assuming next used
                                                                         * line is set up! */

    x->next_free_pipe = 0;
    x->file_next_read_pos = 0L; /* Where to seek to in the file to get more
                                 * data from this chunk to place in the
                                 * buffer */

    /* Note, whenever adding a pipe remember to bump up x->cur_chunk->text */

    /*
     * I *had* been allocating our big block *after* the keys etc had been
     * sorted out, but now because I've bunged everything in together, I have
     * to allocate it first because the key logic uses it.  I just hope the
     * odd bit of space I reserved was enough for all the key records. (*Or*
     * could restructure key stuff not to use global thingmy.  Make keyhead a
     * seperate struct again?)
     */

    if (nfiles == 1) {
        /* -y fred */
        if (argc - i > 0) {
            fprintf(stderr, "sort: only one file allowed with -y option");
            exit(EXIT_FAILURE);
        }
    } else if (i < argc) {
        /*
         * Remainder of argv is list of files to be sorted to single output
         * file...
         */
        nfiles = argc - i;
        if (nfiles == 1) {
            outfile = argv[i];
            sperror(__LINE__, __FILE__, "Warning: assuming sort of %s in place", outfile);
        } else if (nfiles > 1 && gkey.sort_in_place) {
            fprintf(stderr, "sort: -z not relevant when sorting > 1 file");
            gkey.sort_in_place = (0 != 0);
        }
        if ((nfiles == 1) && (gkey.sort_in_place)) {
            /* May scramble input file during the process */
        }
        files = &argv[i];
    } else {
        nfiles = 1;
        files = NULL;
        sperror(__LINE__, __FILE__, "sort: sorting from stdin not yet implemented");
        exit(EXIT_FAILURE);
    }

    if (checkonly) {
        sperror(__LINE__, __FILE__, "sort: check-only option (-c) not implemented at present");
        exit(EXIT_FAILURE);
    }
    if (mergeonly) {
        sperror(__LINE__, __FILE__, "sort: merge-only option (-m) not implemented at present");
        exit(EXIT_FAILURE);
        /*
         * What we really want to do is build up a chunk descriptor for each
         * of the files, and skip the sort stage.  Shouldn't need any
         * temporary files at all.
         */
    }
    /* Work out minimum number of temp files needed for sorting */
    /*
     * Leave the last decision up to sort(), depending on whether file ends
     * up using more than one chunk of memory or not.
     */
    if (gkey.sort_in_place) {
        if (outfile == NULL) {
            sperror(__LINE__, __FILE__, "sort: internal error -- outfile is NULL");
            exit(EXIT_FAILURE);
        }
        ofp = fopen(outfile, "r+");     /* Check output file is writable */
        if (ofp == NULL || fclose(ofp) != 0) {
            sperror(__LINE__, __FILE__, "sort: cannot update file '%s'", outfile /* Had tmp ? */ );
            exit(EXIT_FAILURE);
        }
    } else if (gkey.may_corrupt_infiles) {
        /*
         * sort -z -o yyy  xxx yyy zzz   doesn't need temp file for yyy
         */
    } else if (outfile == NULL) {
        /* Sorting to stdout */
        /*
         * WARNING: if sorting > 1 file to stdout, must sort the files to a
         * temp file, then send the temp file to stdout! (not yet done)
         */
    } else if (nfiles == 1
               && (strcmp(outfile, files[0]) == 0)) {
        gkey.may_corrupt_infiles = (0 == 0);
        gkey.sort_in_place = (0 == 0);
    }
    /*
     * fprintf(stderr, "sort: using %d bytes for buffer\n", x->buffer_lim -
     * x->cur_chunk->text);
     */

    if (outfile == NULL) {
        sperror(__LINE__, __FILE__, "sort: internal error -- outfile is NULL");
        exit(EXIT_FAILURE);
    }
    for (i = 0; i < nfiles; i++) {
        infile = files[i];
        /*
         * cases to test: sort -o yyy xxx yyy zzz, sort -z -o yyy xxx yyy
         * zzz, sort -o yyy yyy, sort -z -o yyy zzz, sort -y yyy, sort -o yyy
         * xxx, sort -z -o yyy xxx.
         */
        if (nfiles == 1 && gkey.sort_in_place) {
            /*
             * sort -y fred
             */
            ofp = fopen(outfile, "rb+");        /* Check in/output file is
                                                 * writable */
            if (ofp == NULL || fclose(ofp) != 0) {
                sperror(__LINE__, __FILE__, "sort: cannot open input/output file '%s'", outfile);
                exit(EXIT_FAILURE);
            }
            sort(infile, outfile, (0 == 0) /* May sort in place */ , (0 != 0));
        } else if (nfiles == 1 && strcmp(outfile, files[i]) != 0) {
            /*
             * sort -o fred jim
             */
            ofp = fopen(outfile, "wb"); /* Check in/output file is writable */
            if (ofp == NULL || fclose(ofp) != 0) {
                sperror(__LINE__, __FILE__, "sort: cannot open output file '%s'", outfile);
                exit(EXIT_FAILURE);
            }
            sort(infile, outfile, gkey.may_corrupt_infiles, (0 != 0));
        } else if (nfiles == 1 && strcmp(outfile, files[i]) == 0) {
            /*
             * sort -o fred fred
             */
            ofp = fopen(outfile, "r+"); /* Check in/output file is writable */
            if (ofp == NULL || fclose(ofp) != 0) {
                sperror(__LINE__, __FILE__, "sort: cannot open input/output file '%s'", outfile);
                exit(EXIT_FAILURE);
            }
            sort(infile, outfile, (0 == 0) /* May sort in place */ , (0 != 0));
        } else if (gkey.may_corrupt_infiles && nfiles > 1 && strcmp(outfile, files[i]) != 0) {
            /*
             * sort -z -o yyy xxx yyy zzz may sort xxx and zzz in place, but
             * not yyy because it has to be merged into.
             */
            ofp = fopen(outfile, "r+"); /* Check in/output file is writable */
            if (ofp == NULL || fclose(ofp) != 0) {
                sperror(__LINE__, __FILE__, "sort: cannot open input/output file '%s'", outfile);
                exit(EXIT_FAILURE);
            }
            sort(infile, infile, (0 == 0) /* May sort in place */ , (0 != 0));
        } else if (strcmp(outfile, files[i]) == 0) {
            /*
             * sort -z -o yyy xxx yyy zzz may sort xxx and zzz in place, but
             * not yyy because it has to be merged into. So sort from yyy to
             * a temp (but allow yyy itself to be used as temp space if need
             * be)
             */

            tmp = tempname();   /* tempname() interface queues temp file for
                                 * deletion on exit */
            if (sort_verbose)
                debugf("Output file %s is also an input file -- will use temp %s\n", outfile, tmp);
            files[i] = tmp;     /* Replace output file with temp copy */
            ofp = fopen(tmp, "wb");     /* Just check it is writable */
            if (ofp == NULL) {
                sperror(__LINE__, __FILE__, "sort: cannot open temp file '%s'", tmp);
                exit(EXIT_FAILURE);
            }
            fclose(ofp);
            sort(infile, tmp, (0 == 0), (0 != 0));
            files[i] = tmp;
        } else {
            /*
             * Copy outfile to a temp file, and substitute the temp file in
             * the input list.  We *should* be able to get away with setting
             * the 'sort-in-place' option and skipping this palaver -- *IFF*
             * there is only one input file.  If there is more than one, we
             * don't want to disturb the others unless we've been given
             * permission.
             */

            tmp = tempname();   /* tempname() interface queues temp file for
                                 * deletion on exit */
            if (sort_verbose)
                debugf("Output file %s is also an input file -- will use temp %s\n", outfile, tmp);
            files[i] = tmp;     /* Replace output file with temp copy */
            ofp = fopen(tmp, "wb");     /* Just check it is writable */
            if (ofp == NULL) {
                sperror(__LINE__, __FILE__, "sort: cannot open temp file '%s'", tmp);
                exit(EXIT_FAILURE);
            }
            fclose(ofp);
            sort(infile, tmp, gkey.may_corrupt_infiles, (0 != 0));
        }
    }                           /* End of for loop over files to be sorted */

    /*
     * At the moment I'm assuming that sort has internally merged each of the
     * files individually.  In fact, if I expand the chunk descriptors to
     * include a filename, I could merge multiple chunks and multiple files
     * in the same operation.  But for the moment I won't bother.
     */
    /* If no of chunks > 1 then ... */
    if (nfiles > 1)
        merge(files, nfiles, outfile);

    free(x);                    /* Isn't tidying up so easy nowadays :-) */
    exit(EXIT_SUCCESS);
    /* NOT REACHED */
    return (0);
}



/*
 * TO DO:
 * 
 * sort_verbose info on merge, input & output options, output to stdout, input
 * from stdin (harder), more reliable file shuffling at end, optimisations
 * for fully-sorted chunks, removal of some of silly options, making -u work,
 * finding out working-set size if possible, better allocation of memory for
 * merging, merging of multi files rather than just one, -m option, special
 * version of compare (see sizeof(LINE) stuff) to get extra memory when
 * simple sort with no keys, ditto using strcmp() if no NULs in text, ...
 */
