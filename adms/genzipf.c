//==================================================== file = genzipf.c =====
//=  Program to generate Zipf (power law) distributed random variables      =
//===========================================================================
//=  Notes: 1) Writes to a user specified output file                       =
//=         2) Generates user specified number of values                    =
//=         3) Run times is same as an empirical distribution generator     =
//=         4) Implements p(i) = C/i^alpha for i = 1 to N where C is the    =
//=            normalization constant (i.e., sum of p(i) = 1).              =
//=-------------------------------------------------------------------------=
//= Example user input:                                                     =
//=                                                                         =
//=   ---------------------------------------- genzipf.c -----              =
//=   -     Program to generate Zipf random variables        -              =
//=   --------------------------------------------------------              =
//=   Output file name ===================================> output.dat      =
//=   Random number seed =================================> 1               =
//=   Alpha vlaue ========================================> 1.0             =
//=   N value ============================================> 1000            =
//=   Number of values to generate =======================> 5               =
//=   --------------------------------------------------------              =
//=   -  Generating samples to file                          -              =
//=   --------------------------------------------------------              =
//=   --------------------------------------------------------              =
//=   -  Done!                                                              =
//=   --------------------------------------------------------              =
//=-------------------------------------------------------------------------=
//= Example output file ("output.dat" for above):                           =
//=                                                                         =
//=   1                                                                     =
//=   1                                                                     =
//=   161                                                                   =
//=   17                                                                    =
//=   30                                                                    =
//=-------------------------------------------------------------------------=
//=  Build: bcc32 genzipf.c                                                 =
//=-------------------------------------------------------------------------=
//=  Execute: genzipf                                                       =
//=-------------------------------------------------------------------------=
//=  Author: Kenneth J. Christensen                                         =
//=          University of South Florida                                    =
//=          WWW: http://www.csee.usf.edu/~christen                         =
//=          Email: christen@csee.usf.edu                                   =
//=-------------------------------------------------------------------------=
//=  History: KJC (11/16/03) - Genesis (from genexp.c)                      =
//===========================================================================
//----- Include files -------------------------------------------------------
#include <assert.h>             // Needed for assert() macro
#include <stdio.h>              // Needed for printf()
#include <stdlib.h>             // Needed for exit() and ato*()
#include <math.h>               // Needed for pow()
#include <time.h>
#include "genzipf.h"

//----- Constants -----------------------------------------------------------
#define  FALSE          0       // Boolean false
#define  TRUE           1       // Boolean true

//===========================================================================
//=  Function to generate Zipf (power law) distributed random variables     =
//=    - Input: alpha and N                                                 =
//=    - Output: Returns with Zipf distributed random variable              =
//===========================================================================
void dump_sum_probs(int n, double *data) {
    const char *format = "zipfian_data_%d.bin";
    char result[255];
    sprintf(result, format, n);
    FILE *f = fopen(result, "w");
    if (!f) {
        fprintf(stderr, "Couldn't dump data to '%s'\n", result);
        exit(5);
    }

    fwrite(data, sizeof(data[0]), n + 1, f);
    fclose(f);
}

void load_sum_probs(int n, double *data) {
    const char *format = "zipfian_data_%d.bin";
    char result[255];
    sprintf(result, format, n);
    FILE *f = fopen(result, "r");
    if (!f) {
        fprintf(stderr, "Couldn't load data from '%s'\n", result);
        exit(6);
    }

    fread(data, sizeof(data[0]), n + 1, f);
    fclose(f);
}

int file_exists(int n) {
    const char *format = "zipfian_data_%d.bin";
    char result[255];
    sprintf(result, format, n);
    FILE *f = fopen(result, "r");
    if (!f) {
        return 0;
    }

    fclose(f);
    return 1;
}

int zipf(double alpha, int n) {
    static int randval_called = FALSE;
    if (!randval_called) {
        rand_val(time(NULL));
        randval_called = TRUE;
    }

    static int first = TRUE;      // Static first time flag
    static double c = 0;          // Normalization constant
    static double *sum_probs;     // Pre-calculated sum of probabilities
    double z;                     // Uniform random number (0 < z < 1)
    int zipf_value;               // Computed exponential value to be returned
    int i;                     // Loop counter
    int low, high, mid;           // Binary-search bounds

    // Compute normalization constant on first call only
    if (first == TRUE) {
        printf("Zipf Pre-Gen Starting\n");
        for (i = 1; i <= n; i++)
            c = c + (1.0 / pow((double) i, alpha));
        c = 1.0 / c;

        sum_probs = (double*)malloc((n + 1) * sizeof(*sum_probs));
        if (file_exists(n)) {
            printf("Loading sum_probs from file\n");
            load_sum_probs(n, sum_probs);
        } else {
            printf("First sum_probs generation for n = %d\n", n);
            sum_probs[0] = 0;
            for (i = 1; i <= n; i++) {
                sum_probs[i] = sum_probs[i - 1] + c / pow((double) i, alpha);
            }
            dump_sum_probs(n, sum_probs);
        }
        printf("Zipf Pre-Gen OK\n");
        first = FALSE;
    }

    // Pull a uniform random number (0 < z < 1)
    do {
        z = rand_val(0);
    } while ((z == 0) || (z == 1));

    // Map z to the value
    low = 1, high = n, mid;
    do {
        mid = floor((low + high) / 2);
        if (sum_probs[mid] >= z && sum_probs[mid - 1] < z) {
            zipf_value = mid;
            break;
        } else if (sum_probs[mid] >= z) {
            high = mid - 1;
        } else {
            low = mid + 1;
        }
    } while (low <= high);

    // Assert that zipf_value is between 1 and N
    assert((zipf_value >= 1) && (zipf_value <= n));

    return (zipf_value);
}

//=========================================================================
//= Multiplicative LCG for generating uniform(0.0, 1.0) random numbers    =
//=   - x_n = 7^5*x_(n-1)mod(2^31 - 1)                                    =
//=   - With x seeded to 1 the 10000th x value should be 1043618065       =
//=   - From R. Jain, "The Art of Computer Systems Performance Analysis," =
//=     John Wiley & Sons, 1991. (Page 443, Figure 26.2)                  =
//=========================================================================
double rand_val(int seed) {
    const long a = 16807;  // Multiplier
    const long m = 2147483647;  // Modulus
    const long q = 127773;  // m div a
    const long r = 2836;  // m mod a
    static long x;               // Random int value
    long x_div_q;         // x divided by q
    long x_mod_q;         // x modulo q
    long x_new;           // New x value

    // Set the seed if argument is non-zero and then return zero
    if (seed > 0) {
        x = seed;
        return (0.0);
    }

    // RNG using integer arithmetic
    x_div_q = x / q;
    x_mod_q = x % q;
    x_new = (a * x_mod_q) - (r * x_div_q);
    if (x_new > 0)
        x = x_new;
    else
        x = x_new + m;

    // Return a random value between 0.0 and 1.0
    return ((double) x / m);
}
