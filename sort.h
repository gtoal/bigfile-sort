/* We could squeeze more lines in when doing a straight alpha sort if we
   just store text & length.  If we do this, though, we must be *extra* careful
   to pass the correct record size around to anyone who needs it!  Actually, we
   could get away with just storing the address if we assume <NUL> termination (or
   even <NL> termination) but we *must* supply a different comparison function
   in that case! */

/* Size to be used when sorting -y0 */
#define DEFAULT_SMALL_MEMORY_SIZE (512*1024)
#define MIN_MEMORY_SIZE (100*1024)


/* line represents a line during the initial sort phase */
/* Lines are held in core as counted strings. */
typedef struct line {
  unsigned char *text;          /* Start of text of this line in buffer */
  size_t length;                /* Length of this line excluding \n */
  /* These records *may* one day be truncated here if doing a simple alpha sort.
     This will allow more lines to be squeezed into Ram.  If we ever do this, we
     must *very* carefully fix everything that knows about sizeof(struct line)
   */
  unsigned char *keybeg;        /* Start of first key */
  unsigned char *keylim;        /* End of first key */
} LINE;

/* Input buffers and sorted chunks. */

/* For the moment, until I know what we need, I'll just use the same info
   as the global format. */
typedef struct chunkstruct
{
/* Add filename and a FILE * perhaps ? */
  unsigned char *buf;           /* Current buffer when merging */
  unsigned char *buf_lim;       /* Limit to free space in buff.
                                   Mostly buffs are going to be completely
                                   full. No point in backtracking to last '\n'.
                                   Once we get to end of last valid line, we'll
                                   have to shunt the remaining partial text
                                   back to the start of the buffer, and read
                                   in the rest of that line and anything
                                   which follows it.
                                 */

  LINE line;                    /* Current line when merging */

  char *stored_chunk_filename;  /* Where the chunk was stored on disk */
  FILE *stored_chunk_file;      /* Haven't decided whether to reopen or not */

  /* These two may be updated as the chunk is consumed. */
  long file_start_pos;          /* Where in the file this chunk starts */
  long file_chunk_length;       /* How long in the file this chunk is */

  /* Am reusing the pointers below to save code. */
  unsigned char *text;
                    /* If sorting: Inclusive start of stored text. */
                    /* If merging: Pointer to first char of current line to be merged. */
  unsigned char *text_accepted;
                    /* If sorting: Exclusive end of stored text (== text+used) */
                    /* If merging: Exclusive pointer to char after '\n' of current line */
  unsigned char *text_lim;
                    /* If sorting: Exclusive upper limit of stored text */
                    /* If merging: Ditto*/
} CHUNK;
#ifdef REMOVING
typedef struct {
   struct pipestruct *p;
   LINE *lookahead;
} OPERAND;
#endif
typedef struct {
  struct pipestruct *left;
                           /* The lookahead lines are just pointers to the lookahead
                              records held by the bottom-level data nodes. (These are
                              actually the 'lookahead_line's of below... -- must make
                              into a union type some time!).  Therefore they can be
                              passed up the tree, but operations on them will affect
                              the lowest level data nodes directly.
                            */

  struct pipestruct *right;
} PAIR;

/* Restriction: can't handle files where a line won't fit in a buffer. */
typedef struct {
  CHUNK chunk;
  LINE head_line;
} SINGLE;

/* (never use 0 for enums - helps catch un-init bugs...) */
#define DATA 1
#define MERGE 2

/* PIPE is used to simulate unix pipes when doing a balanced merge of all the chunks */
typedef struct pipestruct {
  int type;                /* DATA or MERGE */
  union {                  /* Can read from either a merge-node or a chunk in the same way */
    PAIR operand;
    SINGLE chunk;
  } node;
} PIPE;


/* Lists of key field comparisons to be tried. */

/* The head of this list is a field in the global struct. (Actually, it
   used to be a global static, but I'm trying to move as many of the
   globals as I can into the big area).
   I'm seriously considering changing the rest of the elements in
   the list itself from being mallocked items to being an array of
   keyfields stored as the very first set of objects in our big buffer --
   it is entirely empty while key building is going on.  Doing this
   would remove the very last set of random mallocs and give us
   complete control over storage management.
 */
typedef struct keyfield
{
  int sword;                    /* Zero-origin 'word' to start at. */
  int schar;                    /* Additional characters to skip. */
  int skipsblanks;              /* Skip leading white space at start. */
  int eword;                    /* Zero-origin first word after field. */
  int echar;                    /* Additional characters in field. */
  int skipeblanks;              /* Skip trailing white space at finish. */
  int *ignore;                  /* Boolean array of characters to ignore. */
  unsigned char *translate;     /* Translation applied to characters. */
  int numeric;                  /* Flag for numeric comparison. */
  int month;                    /* Flag for comparison by month name. */
  int reverse;                  /* Reverse the sense of comparison. */
  int sort_in_place;            /* If true means input file is also output file. */
  int may_corrupt_infiles;      /* If true allows input files to be used as temps. */
  int delete_input_files;       /* If true means delete input files as soon as they
                                   have been used, to save disk space */
  struct keyfield *next;        /* Next keyfield to try. */
} KEYFIELD;

/* We have *one* 'struct global' to describe the permanent state of memory
   and the current chunk in ram being sorted.  We have several 'struct chunk's
   describing the sorted chunks.  These are used in the merge phase.
   Although this is off the general heap, it could as easily be the first
   item in our big buffer.  (I'm also considering doing away with 'chunk's
   in the buffer and making them straight into PIPEs) */
typedef struct globalstruct
{
  /* Line* below will shortly be made into a line[], -- it MUST be
     the first field of this struct because the space claimed for
     this struct has been tweaked to an exact multiple of LINE size.
     Lines are used from the high indexes downwards, and checks should
     ensure that they never get anywhere near element zero...
   */
  LINE line[1];                 /* Overlaid with array of line descriptors */
                                /* This is an array whose bounds are deliberately exceeded */
  /* --- NO CHANGES BEFORE THIS LINE --- */
  int line_base;                /* Numerically highest line array index (exclusive) */

  /* THIS IS CURRENTLY HOW next_used_line WORKS:  you decrement it first, then it
     is valid for use.  Except when 'next_used_line == line_base', line[next_...]
     is always valid */
  int next_used_line;           /* Next free slot when working downwards (exclusive) */
                                /*                                 (or possibly not) */

  /* I'm giving serious consideration to making 'next_used_line' a downwards-empty
     stack instead of the current downwards-full stack.  At the moment, the element
     indexed by next_used_line is always valid -- you decrement it then immediately
     put things in it.  If I change it, it will always point to an empty slot, and
     you will then instead fill it and decrement it immediately afterwards.  The advantage
     of this is that an obscure case to do with long lines becomes less of a problem.
     It also fulfils my aesthetic criterion of lower-bound-inclusive/upper-bound-exclusive,
     though you have to stand on your head to see it ;-) [The criterion is reversed
     for a downwards growing stack...]
   */


  char *current_infile_name;    /* I've just added these four fields to see */
  FILE *current_infile_fd;      /* whether they help with multi-buffer stuff */
  long file_next_read_pos;      /* Where to seek to in the file to get more
                                   data from this chunk to place in the buffer */
  long file_length;             /* Number of bytes used in file.  Bit of a hack. */
  int  cr_was_missing;          /* Did last line not end in \n? */
  char *current_outfile_name;
  FILE *current_outfile_fd;
  long  outfile_next_write_pos;

  CHUNK *cur_chunk;             /* pointer to current chunk descriptor
                                   -- updated every time round the sort loop */
#ifdef _Ive_moved_these_to_the_chunk
  unsigned char *text;          /* Inclusive start of stored text. */
  unsigned char *text_accepted;
                                /* Exclusive end of stored text (== text+used) */
  unsigned char *text_lim;      /* Exclusive upper limit of stored text */
#endif                                
  unsigned char *buffer_lim;    /* Exclusive limit of area allowed for text. Need not
                                   all be used.  This is automatically dropped every
                                   time a new line record is created.  It could
                                   equally be called 'line_base_ptr', and is just
                                   a 'char *' equivalent for "&x->line[x->next_used_line]"
                                   [But: see discussion on next_used_line above]
                                 */
                                /* This struct is claimed from the heap with a very
                                   large malloc -- the 'pipe[]' array effectively
                                   being a flex array -- it extends past the first
                                   element.  However most of the area between pipes
                                   growing thataway and the lines growing thisaway is
                                   filled with the text from the file itself...
                                 */
  int next_free_pipe;           /* Exclusive index of next pipe to be claimed */

  /* --- NO CHANGES AFTER THIS LINE --- */
  PIPE pipe[1];                 /* Yet another flex array taken from our buff. Follows
                                   on immediately from here... */

} GLOBAL;

int sort_debug = (0!=0);        /* voluminous diags when debugging */
int sort_verbose = (0!=0);      /* Extra diags when -v */
int field_sep_char = ' ';       /* Default parameter for -tc */
size_t kmem = -1;               /* How much memory to allocate. (-1 means max) */

/* Remember names of temp files so we can clean up on exit */
typedef struct tempobj {
    char *fname;
    struct tempobj *next;
} tempobj;


/* Two useful macros. */
#define MIN(a, b) ((a) < (b) ? (a) : (b))
#define UCHAR(c) ((unsigned char) (c))

static int digits[UCHAR_MAX+1];          /* Table of digits. */

static int blanks[UCHAR_MAX+1];          /* Table of white space. */

static int nonprinting[UCHAR_MAX+1];     /* Table of non-printing characters. */

static int nondictionary[UCHAR_MAX+1];   /* Table of non-dictionary characters
                                            (not letters, digits, or blanks). */

static unsigned char fold_tolower[UCHAR_MAX+1];  /* Translation table folding upper case
                                                    to lower. */

/* Table mapping 3-letter month names to integers.
   Alphabetic order allows binary search. */
static struct month {
  char *name;
  int val;
} monthtab[] = {
  "apr", 4,
  "aug", 8,
  "dec", 12,
  "feb", 2,
  "jan", 1,
  "jul", 7,
  "jun", 6,
  "mar", 3,
  "may", 5,
  "nov", 11,
  "oct", 10,
  "sep", 9
};


/* Flag to reverse the order of all comparisons. */
static int reverse = (0!=0); /* (Forward by default) */

/* Tab character separating fields.  If NUL, then fields are separated
   by the empty string between a non-whitespace character and a whitespace
   character. */
static unsigned char tab = '\0'; /* (might as well be explicit...) */

/* Flag to remove consecutive duplicate lines from the output.
   Only the last of a sequence of equal lines will be output. */

/* Note that my hacks have broken -u because I write back a file (or leave
   it untouched) if it was already sorted.  I could bodge this by having
   the 'is sorted?' test return false if both a pair of consecutive lines
   were equal, _and_ the 'unique' flag is true. */

static int unique = (0!=0); /* (don't remove dups by default) */
static int unsort = (0!=0); /* (don't remove dups by default) */

GLOBAL *x = NULL;  /* Our major eXternal global data structure... */
KEYFIELD keyhead;  /* Head of list of keys */


/* Generate these headers with "mkptypes -s -e -A -i sort.h sort.c"
   (This is my hack of mkptypes which lets you update a 'header section'
    in place in a proper header file) */

/* Headers for sort.c */
static void debugf(const char *fmt, ...);
static void sperror(int line, char *file, const char *fmt, ...);
static void inittables(void);
static void display_mem_usage(char *where);
static PIPE *new_pipe(void);
extern PIPE *make_pipechunk(void);
extern PIPE *make_pipeoper(void);
static PIPE *merge_pipes(PIPE *p, PIPE *left, PIPE *right);
static void display_chunk(CHUNK *c, char *why);
static void replenish_chunk(CHUNK *chunk);
static void preload_chunk_cache(int next_free_chunk);
static PIPE *merge_chunks(void);
static int preload_line(CHUNK *c);
extern int preload_next_line(PIPE *p, CHUNK **chunkp, int (*compare )(const void *l, const void *r ));
static void consume_pipes(PIPE *root, FILE *outf);
static unsigned char *begfield(const LINE *line, struct keyfield *key);
static unsigned char *limfield(const LINE *line, struct keyfield *key);
static int trim_newline(void);
static void debug_crosscheck(char *s);
static void findlines(void);
static int fraccompare(const unsigned char *a, const unsigned char *b);
static int numcompare(const unsigned char *a, const unsigned char *b);
static int getmonth(unsigned char *s, int len);
static int keycompare(const LINE *a, const LINE *b);
extern int compare(const void *av, const void *bv);
static int checklines(LINE *lines, int nlines, int (*compare )(const void *l, const void *r ));
static void shuffle(LINE *lines, int nlines);
static void reverse_lines(LINE *lines, int nlines);
static void insertkey(struct keyfield *key);
extern void wipe_keys(void);
extern void wipe_temps(void);
static char *tempname(void);
static void usage(void);
static void badfieldspec(char *s);
extern size_t freemem(void);
static unsigned char *largest_malloc(size_t *size);
static void merge(char **files, int nfiles, char *outfile);
static void writeout(FILE *f, long startoffset);
static void savechunk(CHUNK *c, char *fname, FILE *f, long startoffset, long no_of_bytes);
extern void readlines(FILE *in);
extern int perform_sort(int order);
static int get_and_sort_block(FILE *in, long startoffset, long file_length, int *optimflag);
static int copy_back(char *infile, char *outfile);
static void sort(char *infile, char *outfile, int can_sort_in_place, int can_delete_input);
static void memsize(char *mems, size_t *kmem);
static void HANDLE_PARAMETERS(int *argcp, char ***argvp, struct keyfield **keyp, struct keyfield *gkeyp, int *mergeonlyp, int *checkonlyp, char **outfilep, int *nfilesp, char ***filesp, int *ip);
extern int main(int argc, char **argv);
/* End of headers for sort.c */


