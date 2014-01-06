classdef MasterPairWiseSPMD < legion.Master
    
    properties
        % Amount of times to split input data
        num_data_splits;
        save_kernel;
    end
    
    methods( Access=public )
        
        function obj = MasterPairWiseSPMD( kernel, data, numProcCores, numDataSplits, jobName )
            obj@legion.Master( kernel, data, numProcCores, jobName );
            obj.num_data_splits = numDataSplits;
            obj.setSaveKernel( legion.MasterPairWiseSPMD.default_saveKernel );
        end
        
        %{
        idx is the number processing thread
        %}
        function exec = executable( obj, idx )
            exec = ['legion.JobRunnerPairWiseSPMD( ', num2str( idx ), ', ''', obj.jobPath, ''', ', num2str( obj.num_data_splits ) ,', ',num2str( obj.numcores ),'  );' ];
        end
        
        function setSaveKernel( obj, kernel )
            obj.save_kernel = kernel;
            legion.MasterPairWiseSPMD.save_saveKernel( kernel, obj.jobPath );
        end
        
        function run( obj , doPrepare )
            
            if( nargin == 1 )
                doPrepare = 1;
            end
            
            if( doPrepare )
                obj.prepare();
            end
            
            for i = 1:obj.numcores
                obj.grid.setExecutable( obj.executable( i ), i );
                obj.grid.executePBS( i );
            end
            disp('Saving master template...');
            legion.Master.saveMaster( obj  );
        end
        
        function numsplits = prepare( obj )
            
            [numsplits, splitSizes] = legion.Master.split( size(obj.data, 1), obj.num_data_splits );
            disp(['Preparing to split into : ',num2str(numsplits), ' blocks']);
            legion.Master.saveSplits( splitSizes, obj.data, obj.jobPath, obj.preprockernel );
            legion.Master.saveKernel( obj.kernel, obj.jobPath );
            legion.MasterPairWiseSPMD.save_saveKernel( obj.save_kernel, obj.jobPath );
        end
        
    end
    
    methods ( Static )
        
        function save_saveKernel( kernel, jobPath )
           save( [jobPath,'save_kernel.mat'], 'kernel',  '-v7.3' ); 
        end
        
        function kernel = load_saveKernel( jobPath )
            loaded = load( [jobPath,'save_kernel.mat'] );
            kernel = loaded.kernel;
        end
        
        function kernel = default_saveKernel()
            kernel = legion.Kernel;
            kernel.add( @legion.MasterPairWiseSPMD.saveBlockFromStruct, 'X' );
        end
        
        function nothing = saveBlockFromStruct( input )
            
            dataPath        = input.dataPath;
            jrID            = input.jrID;
            r_block_idx     = input.r_block_idx;
            c_block_idx     = input.c_block_idx;
            output          = input.output;
            rowidxs         = input.rowidxs;
            colidxs         = input.colidxs;
            
            filename = [dataPath,'output',num2str( r_block_idx ), '_', num2str( c_block_idx ),'.mat'];
            fprintf( 'Saving: %s\n', filename );
            save( filename, 'output', 'r_block_idx', 'c_block_idx', 'jrID', 'rowidxs', 'colidxs' , '-v7.3' );
            nothing = 0;
        end
        
    end
    
end