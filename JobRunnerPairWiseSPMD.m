classdef JobRunnerPairWiseSPMD < legion.Jobrunner
    
    properties
        numsplits
        numprocs
        save_kernel;
    end
    
    methods
        
        % Pair-wise, no-overlap (lower triangle) distributed job running
        function obj = JobRunnerPairWiseSPMD( jrID, dataPath, numsplits, numprocs )
            obj@legion.Jobrunner( jrID, dataPath, 1 ); %parent, no execute
            obj.numsplits   = numsplits;
            obj.numprocs    = numprocs;
            obj.save_kernel = legion.MasterPairWiseSPMD.load_saveKernel( dataPath );
            obj.localRunner();
        end
        
        function localRunner( obj )
            
            fprintf( 'Executing Distr Runner ID: %i\n', obj.jrID );
            grid        = legion.JobRunnerPairWiseSPMD.calcGrid( obj.numsplits, obj.numprocs )
            blocks      = legion.JobRunnerPairWiseSPMD.getBlocks( obj.jrID, grid );
            
            warning off;
            obj.kernel.estEnvironment();
            
            tic;
            for total_idx = 1:size(blocks,1)
                
                idxs = blocks(total_idx,:);
                r = idxs(1);
                c = idxs(2);
                
                [r_data, r_startidx, r_idxs] = obj.load( obj.dataPath, r );
                [c_data, c_startidx, c_idxs] = obj.load( obj.dataPath, c );
                
                fprintf( '------------------------------------------\n');
                fprintf( 'Computing All-To-All for blocks: r%i & c%i\n',  r, c );
                fprintf( '\tProcess: %i of %i\n',  total_idx, size(blocks,1) );
                
                
                output = cell( size(r_data,1), size(c_data,1) );
                
                % LOOP ROW BLOCK
                for j = 1:size(r_data,1)
                    
                    % LOOP COL BLOCK
                    for i = 1:size(c_data,1)
                        
                        obj.kernel.initial( [ r_data(j,:); c_data(i,:)] );
                        output{j,i} = obj.kernel.execute();
                        
                    end %COL BLOCK
                    
                end % ROW BLOCK
                
                time = toc;
                fprintf( '\n\tTotal Elapsed Time: %.2f (s)\n',  time );
                fprintf( '\n\t    Last Iter Time: %.2f (s)\n',  time/size(blocks,1) );
                
                params = legion.JobRunnerPairWiseSPMD.saveParamsToStruct( obj.dataPath, obj.jrID, r, c, output,  r_idxs, c_idxs );
                
                obj.save_kernel.initial( params );
                obj.save_kernel.execute();
                
                fprintf( '------------------------------------------\n\n');
                
            end
            
        end
        
    end
    
    
    methods ( Static )
       
        %{ 
        Calculate responsibilities for each processor determinstically
        across processors
        
        uses Md5 hash algorithm of key values in block grid
        %}
        function grid = calcGrid( numsplits, numprocs )
           
            grid = zeros( numsplits, numsplits );
            
            for i = 1:numsplits
                for j = 1:numsplits
                    if( j > i ); continue; end;
                    grid(i,j) = 1;
                end
            end
            
            lst = find( grid == 1 );
            
            proc_no = -1 .* ones( 1, length(lst) );
            
            hash.Method = 'MD5';
            hash.Format = 'double';
            
            for i = 1:length(lst)
                proc_no(i) = mod( sum(legion.DataHash( lst(i), hash )), numprocs )+1;
            end
            
            grid( lst ) = proc_no;
            
            
        end
        
        function blocks = getBlocks( jrID, grid )
            
            [r,c] = find( grid == jrID );
            blocks = [r,c];
            
        end
        
        function saveBlock( dataPath, jrID, r_block_idx, c_block_idx, output, rowidxs, colidxs )
            filename = [dataPath,'output',num2str( r_block_idx ), '_', num2str( c_block_idx ),'.mat'];
            fprintf( 'Saving: %s\n', filename );
            save( filename, 'output', 'r_block_idx', 'c_block_idx', 'jrID', 'rowidxs', 'colidxs' , '-v7.3' );
        end
        
        function params = saveParamsToStruct( dataPath, jrID, r_block_idx, c_block_idx, output, rowidxs, colidxs )
            
            params.dataPath     = dataPath;
            params.jrID         = jrID;
            params.r_block_idx  = r_block_idx;
            params.c_block_idx  = c_block_idx;
            params.output       = output;
            params.rowidxs      = rowidxs;
            params.colidxs      = colidxs;
            
        end
        
    end
end