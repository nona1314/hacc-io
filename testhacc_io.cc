#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <fcntl.h>
#include "mpi.h"
#include <iostream>

#include "restartio_glean.h"

using namespace std;

int main (int argc, char * argv[]) 
{
    char* fname = 0;    
    char* buf = 0;
    int numtasks, myrank, status;
    MPI_File fh;

    status = MPI_Init(&argc, &argv);
    if ( MPI_SUCCESS != status)
    {
        printf(" Error Starting the MPI Program \n");
        MPI_Abort(MPI_COMM_WORLD, status);
    }

    MPI_Comm_size(MPI_COMM_WORLD, &numtasks);
    MPI_Comm_rank(MPI_COMM_WORLD, &myrank);

    if (argc != 3)
    {
        printf (" USAGE <exec> <particles/rank>  < Full file path>  ");
        MPI_Abort(MPI_COMM_WORLD, -1);
    }
    
    int64_t num_particles =  atoi(argv[1]);
    
    fname = (char*)malloc(strlen(argv[2]) +1);
    strncpy (fname, argv[2], strlen(argv[2]));
    fname[strlen(argv[2])] = '\0';
    
#ifndef HACC_IO_DISABLE_WRITE
    // Let's Populate Some Dummy Data
    float *xx, *yy, *zz, *vx, *vy, *vz, *phi;
    int64_t* pid;
    uint16_t* mask;
    
    void *xx1, *yy1, *zz1, *vx1, *vy1, *vz1, *phi1, *pid1, *mask1;

    int64_t page_num_float = (num_particles * sizeof(float) + 4095) / 4096;
    int64_t page_num_int64 = (num_particles * sizeof(int64_t) + 4095) / 4096;
    int64_t page_num_uint16 = (num_particles * sizeof(uint16_t) + 4095) / 4096;
    int mem_err;
    mem_err = posix_memalign(&xx1, 4096, page_num_float * 4096);
    if(mem_err != 0) { cout << mem_err << endl; return -1;}
    mem_err = posix_memalign(&yy1, 4096, page_num_float * 4096);
    if(mem_err != 0) { cout << mem_err << endl; return -1;}
    mem_err = posix_memalign(&zz1, 4096, page_num_float * 4096);
    if(mem_err != 0) { cout << mem_err << endl; return -1;}
    mem_err = posix_memalign(&vx1, 4096, page_num_float * 4096);
    if(mem_err != 0) { cout << mem_err << endl; return -1;}
    mem_err = posix_memalign(&vy1, 4096, page_num_float * 4096);
    if(mem_err != 0) { cout << mem_err << endl; return -1;}
    mem_err = posix_memalign(&vz1, 4096, page_num_float * 4096);
    if(mem_err != 0) { cout << mem_err << endl; return -1;}
    mem_err = posix_memalign(&phi1, 4096, page_num_float * 4096);
    if(mem_err != 0) { cout << mem_err << endl; return -1;}
    mem_err = posix_memalign(&pid1, 4096, page_num_int64 * 4096);
    if(mem_err != 0) { cout << mem_err << endl; return -1;}
    mem_err = posix_memalign(&mask1, 4096, page_num_uint16 * 4096);
    if(mem_err != 0) { cout << mem_err << endl; return -1;}
    xx = (float*)xx1;
    yy = (float*)yy1;
    zz = (float*)zz1;
    vx = (float*)vx1;
    vy = (float*)vy1;
    vz = (float*)vz1;
    phi = (float*)phi1;
    pid = (int64_t*)pid1;
    mask = (uint16_t*)mask1;
    
    for (uint64_t i = 0; i< num_particles; i++)
    {
        xx[i] = (float)i;
        yy[i] = (float)i;
        zz[i] = (float)i;
        vx[i] = (float)i;
        vy[i] = (float)i;
        vz[i] = (float)i;
        phi[i] = (float)i;
        pid[i] =  (int64_t)i;
        mask[i] = (uint16_t)myrank;
    }
#endif

    RestartIO_GLEAN* rst = new RestartIO_GLEAN();
    
    rst->Initialize(MPI_COMM_WORLD);

    rst->SetPOSIX_IO_Interface(1);
    rst->DisablePreAllocateFile();
    rst->DisablePreFillFile();

#ifndef HACC_IO_DISABLE_WRITE
    rst->CreateCheckpoint (fname, num_particles);
    
    rst->Write(xx, yy, zz, vx, vy, vz, phi, pid, mask);
        
    rst->Close();
#endif

#ifndef HACC_IO_DISABLE_READ
    // Let's Read Restart File Now
    
    float *xx_r, *yy_r, *zz_r, *vx_r, *vy_r, *vz_r, *phi_r;
    int64_t* pid_r;
    uint16_t* mask_r;
    int64_t my_particles;
    
    my_particles  = rst->OpenRestart (fname);
    
    if (my_particles != num_particles)
    {
        cout << " Particles Counts Do NOT MATCH " <<  endl;
        MPI_Abort(MPI_COMM_WORLD, -1);
    }
    
    rst->Read( xx_r, yy_r, zz_r, vx_r, vy_r, vz_r, phi_r, pid_r, mask_r);

    rst->Close();

#ifndef HACC_IO_DISABLE_WRITE
    // Verify the contents if we have the original values stored in memory
    for (uint64_t i = 0; i< num_particles; i++)
    {
        if ((xx[i] != xx_r[i]) || (yy[i] != yy_r[i]) || (zz[i] != zz_r[i])
            || (vx[i] != vx_r[i]) || (vy[i] != vy_r[i]) || (vz[i] != vz_r[i])
            || (phi[i] != phi_r[i])|| (pid[i] != pid_r[i]) || (mask[i] != mask_r[i]))
        {
            cout << " Values Don't Match Index:" << i <<  endl;
            cout << "XX " << xx[i] << " " << xx_r[i] << " YY " << yy[i]  << " " << yy_r[i] << endl;
            cout << "ZZ " << zz[i] << " " << zz_r[i] << " VX " << vx[i]  << " " << vx_r[i] << endl;
            cout << "VY " << vy[i] << " " << vy_r[i] << " VZ " << vz[i]  << " " << vz_r[i] << endl;
            cout << "PHI " << phi[i] << " " << phi_r[i] << " PID " << pid[i]  << " " << pid_r[i] << endl;
            cout << "Mask: " << mask[i] << " " << mask_r[i] << endl;
            
            MPI_Abort (MPI_COMM_WORLD, -1);
        }
    }
#endif
    
    MPI_Barrier(MPI_COMM_WORLD);
    
#ifndef HACC_IO_DISABLE_WRITE
    if (0 == myrank)
        cout << " CONTENTS VERIFIED... Success " << endl;
#endif
#endif
    
    rst->Finalize();

    delete rst;
    rst = 0;
    
#ifndef HACC_IO_DISABLE_WRITE
    // Delete the Arrays
    free(xx1);

    free(yy1);
    free(zz1);
    free(vx1);
    free(vy1);
    free(vz1);
    free(phi1);
    free(pid1);
    free(mask1);
#endif

#ifndef HACC_IO_DISABLE_READ
    delete []xx_r;
    delete []yy_r;
    delete []zz_r;
    delete []vx_r;
    delete []vy_r;
    delete []vz_r;
    delete []phi_r;
    delete []pid_r;
    delete []mask_r;
#endif
    MPI_Finalize();

    return 0;
}

