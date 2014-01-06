classdef DistrJobrunner < legion.Jobrunner
    
    properties
        numprocs;
    end
    
    methods
        
        % Pair-wise, no-overlap (lower triangle) distributed job running
        function obj = DistrJobrunner( jrID, dataPath, numprocs )
            obj@legion.Jobrunner( jrID, dataPath, 1 ); %parent, no execute
            obj.numprocs = numprocs;
            obj.localRunner();
        end
        
        function localRunner( obj )
            
            fprintf( 'Executing Distr Runner ID: %i\n', obj.jrID );
            grid = legion.DistrJobrunner.calcLoadGrid( obj.numprocs )
            
            % Which loads this processor is responsible for along this
            % column
            procGrid        = grid( :, obj.jrID );
            didxs           = find( procGrid == obj.jrID );
            
            % Determine any row responsibilities
            procGridRow     = grid( obj.jrID, : );
            rdidxs          = find( procGridRow == obj.jrID );
            didxr           = find( rdidxs ~= obj.jrID );
            
            % Early return for debugging
            %return;
            warning off;
            
            % Execute the kernel enviornment
            obj.kernel.estEnvironment();
            
            % Create an output variable
            output          = cell(0);
            block           = obj.jrID;
            
            % New variable for this runner's data original responsiblity
            origdata        = obj.data;
            origidxs        = obj.thisidxs;
            curidxs         = origidxs;
            maxorigvertex   = size(obj.data,1);
            
            %
            %   Compute All col analysis
            %
            for k = didxs'
                output = cell(0);
                fprintf( '------------------------------------------\n');
                fprintf( 'Computing All-To-All for blocks: r%i & c%i\n',  k, obj.jrID );
                fprintf( '------------------------------------------\n');
                % the jrID data is loaded in the jobRunner parent
                % constructor and that should always be the first didxs
                if( k ~= obj.jrID )
                    fprintf( '--------------------------------\n');
                    fprintf( 'Loading block: %i\n', k );
                    fprintf( '--------------------------------\n\n');
                    [obj.data, obj.startidx, curidxs]  = obj.load( obj.dataPath, k );
                    block=k;
                end
                
                %Loop over local primary block data vertices
                for j = 1:maxorigvertex
                    
                    fprintf('Primary-Vertex: %i (row-block %i) / %i (col-block %i)\n', j, k, maxorigvertex, obj.jrID); 
                    % Loop over new data vertices
                    maxvertex = size(obj.data,1);
                    for i = 1:maxvertex
                        % Display output
                        if( maxvertex < 100 )
                            fprintf('\tVertex: %i / %i \n', i, maxvertex ); 
                        else
                            % only display on 10% intervals
                            if( mod( i, round( maxvertex/10 ) ) == 0 )
                                fprintf('\tVertex: %i / %i \n', i, maxvertex ); 
                            end
                        end
                        %[origdata(j,:); obj.data(i,:) ]
                        %size( [origdata(j,:); obj.data(i,:) ] );
                        %obj.kernel.initial( [origdata(j,:); obj.data(i,:) ] );
                        
                        % Note that this is inserted as [row -idxs; col-idxs]
                        obj.kernel.initial( [ origdata(j,:); obj.data(i,:)] );
                        output{i,j} = obj.kernel.execute();
                        %output{j}{i}
                    end
                    
                end
                
                disp(['saving block: ', num2str( k ), ', ', num2str( obj.jrID )]);
                legion.DistrJobrunner.saveGrid( obj.dataPath, k, obj.jrID, output, curidxs, origidxs );
                
            end
            
            %
            %   Compute All non-col analysis
            %
            for k = didxr
                output = cell(0);
                % the jrID data is loaded in the jobRunner parent
                % constructor and that should always be the first didxs
                if( k ~= obj.jrID )
                    [obj.data, obj.startidx, curidxs]  = obj.load( obj.dataPath, k );
                    block=k;
                end
                
                maxvertex = size(obj.data,1);
                
                %Loop over the origianl data
                for j = 1:size(origdata,1)
                    
                    fprintf('Secondary-Vertex: %i (col-block %i) / %i (row-block %i)\n', j, obj.jrID, size(origdata,1), block); 
                    
                    % Loop over the current data block
                    for i = 1:maxvertex
                        fprintf('\tVertex: %i / %i\n', i, maxvertex); 
                        %size( [origdata(j,:); obj.data(i,:) ] );
                        obj.kernel.initial( [ obj.data(i,:); origdata(j,:) ] );
                        output{j,i} = obj.kernel.execute();
                    end
                    
                end
                
                disp(['saving block: ', num2str( obj.jrID ), ', ', num2str( k )] );
                legion.DistrJobrunner.saveGrid( obj.dataPath, obj.jrID, k, output,  origidxs, curidxs);
                
            end
            
            %{
            output = cell(0);
            obj.kernel.estEnvironment();
            maxvertex = size(obj.data,1);
            for i = 1:size(obj.data,1)
                fprintf('Vertex: %i / %i\n', i, maxvertex); 
                obj.kernel.initial( obj.data(i,:) );
                output{i} = obj.kernel.execute();
            end
            disp('saving...');
            legion.Jobrunner.save( obj.dataPath, obj.jrID, output );
            %}
            warning on;
            
        end
        
        function computeBlocks( blockIdx )
            
        end
        
    end
    
    methods (Static)
        
        function [grid,splits] = calcLoadGrid( numProcs )
            
            grid = zeros( numProcs, numProcs );
            % LoadableSquares are the number of non-diagonal processes to
            % complete
            loadableSquares = ( (numProcs * numProcs ) - numProcs ) /2;
            
            % Acquire the modified number of processors to use, and the
            % dividision of blocks amonst those processors
            [procs, sizes] = legion.Master.split( loadableSquares, numProcs );
            
            % Loop over the processor count
            for i = 1:numProcs
                
                % Set the diagonal to always be the processor
                grid( i,i ) = i;
                process = 1;
                
                % Move down the columns (j = col value)
                for j = i+1:numProcs
                    
                    % Ensure we still have authority to use this node for
                    % values
                    if( process )
                        % All available nodes that we can use
                        %lst = find( sizes > 0 );
                        %nxtUnit = lst(1);
                        
                        % Check that the next available node is the current
                        % column index
                        %if( i ~= nxtUnit )
                        %    continue;
                        %end
                        
                        grid( j,i ) = i;
                        sizes( i ) = sizes( i ) - 1;
                        if( sizes(i ) == 0 )
                            process = 0;
                        end
                        
                    end
                end
            end
            
            grid;
            sizes;
            
            for i = 1:length(sizes)
                if( sizes(i) > 0 )
                    for k = 1:length(grid)
                        if( k > i || k == i ); continue; end;
                        if( grid( i, k ) == 0 )
                            grid( i, k ) = i;
                            sizes( i ) = sizes( i ) - 1;
                        end
                    end
                end
            end
            
            splits = [];
            % recompute each nodes number
            for i = 1:numProcs
                splits = [splits; length(find( grid==i )) ];
            end
            
            
            
        end
        
        function saveGrid( dataPath, rowBlock, jrID, output, rowidxs, colidxs )
            save( [dataPath,'output',num2str( rowBlock ), '_', num2str( jrID ),'.mat'], 'output', 'rowBlock','jrID', 'rowidxs', 'colidxs' , '-v7.3' );
        end
        
        function [row, col] = extractBlock( saveName )
            row = -1; col = -1;
            s = saveName;
            s = s(1:end-4);
            lst = find( s == '_' );
            
            if( lst == 8 )
                % Single digit row
                row = s(7);
            else
                row = [s(7),s(8)];
            end
            
            col = s(lst+1:end);
            row = str2num(row);
            col = str2num( col );
            
        end
        
    end
    
end