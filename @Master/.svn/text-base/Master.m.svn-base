classdef Master < handle
    
    properties
        kernel;
        data;
        numcores;
        jobName;
        jobPath;
        grid;
        realnumcores;
        preprockernel;
    end
    
    
    methods
        
        % Create a new master system
        function obj = Master( kernel, data, numcores, jobName )
            obj.kernel      = kernel;
            obj.data        = data;
            obj.numcores    = numcores;
            obj.jobName     = jobName;
            if( jobName ~= 0 )
                obj.grid        = legion.Grid( jobName );
                obj.jobPath     = obj.grid.getJobPath();
            else
                obj.grid        = legion.Grid();
            end
            obj.preprockernel = legion.Kernel;
        end
        
        function use_existing_job( obj, jobName )
           obj.grid.setExistingJob( jobName ); 
           obj.jobPath     = obj.grid.getJobPath();
        end
        
        
        % Run the job on the cluster
        function run( obj , doPrepare )
            
            if( nargin == 1 )
                doPrepare = 1;
            end
            
            if( doPrepare )
                obj.realnumcores = obj.prepare();
            end
            
            for i = 1:obj.realnumcores
                obj.grid.setExecutable( obj.executable( i ), i );
                obj.grid.executePBS( i );
            end
            
            disp('Saving master template...');
            legion.Master.saveMaster( obj  );
        end
        
        function rerun( obj )
           obj.run( 0 ); 
        end
        
        % Prepare the data on the cluster
        function numProcs = prepare( obj )
            
            [numProcs, splitSizes] = legion.Master.split( size(obj.data, 1), obj.numcores );
            disp(['Preparing to run on: ',num2str(numProcs), ' cores']);
            legion.Master.saveSplits( splitSizes, obj.data, obj.jobPath, obj.preprockernel );
            legion.Master.saveKernel( obj.kernel, obj.jobPath );
            
        end
        
        function setPreProcKernel( obj, kernel )
            obj.preprockernel = kernel;
        end
        
        function exec = executable( obj, idx )
            exec = ['legion.Jobrunner( ', num2str( idx ), ', ''', obj.jobPath, ''' );' ];
        end
        
        function rOutput = reintegrate( obj )
            
            %cd( obj.jobPath );
            out = dir( 'output*' );
            in = dir( 'input*' );
            
            if( length(out) ~= obj.realnumcores )
                error('Reintegration not possible. All data has not been processed' );
            end
            
            rOutput = cell(0);
            
            for i = 1:length( out )
                
                output = load( out(i).name );
                input = load( in(i).name );
                output = output.output;
                startidx = input.startidx;
                count = 1;
                for j = 1:length(output )
                    rOutput{startidx+j-1} = output{j};
                end
            end
            
            
        end
        
        
        
    end
    
    methods (Static)
        
        function saveSplits( splitSizes, data, jobPath, preprocKernel )
            
            splitSizes = [1, splitSizes];
            for i = 2:length(splitSizes)
                startidx    = sum( splitSizes(1:i-1) );
                idxs        = sum( splitSizes(1:i-1) ):sum( splitSizes(1:i) )-1;
                input       = data( sum( splitSizes(1:i-1) ):sum( splitSizes(1:i) )-1, : );
                
                % Execute preprocess kernel before saving
                fprintf( 'Preprocessing Kernel...' );
                preprocKernel.initial( input );
                input       = preprocKernel.execute();
                
                fprintf( 'Saving input block: %i of %i\n', i-1, length(splitSizes)-1 );
                save( [jobPath,'input',num2str(i-1),'.mat'], 'input', 'startidx', 'idxs' );
            end
            
        end
        
        function saveKernel( kernel, jobPath )
            save( [jobPath,'kernel.mat'], 'kernel',  '-v7.3' );
        end
        
        function [ numprocesses, splitSizes ] = split( numVertices, numcores )
            
            if( numVertices < numcores )
                numprocesses = numVertices;
            else 
                numprocesses = numcores;
            end
            
            if( rem( numVertices, numprocesses ) == 0 )
                maxPerProcs = numVertices / numprocesses;
                splitSizes = ones(1, numprocesses) .* maxPerProcs;
            else
                maxPerProcs = floor(numVertices / numprocesses);
                remain = rem( numVertices, numprocesses );
                
                if( remain > 0 )
                    
                    splitSizes = ones(1, numprocesses)*maxPerProcs;
                    for i = 1:remain
                        splitSizes(i) = splitSizes(i) + 1;
                    end
                else
                    splitSizes = [ ones(1, numprocesses-1) .* maxPerProcs, maxPerProcs+remain ];
                end
                
                %splitSizes = [ ones(1, numprocesses-1) .* maxPerProcs, maxPerProcs+remain ];
                if( sum( splitSizes ) ~= numVertices )
                    error('Missing Data During Split');
                end
            end
            
        end
        
        function saveMaster( master )
            save( [master.jobPath, 'master.mat'], 'master', '-v7.3');
        end
        
        
        
    end
    
    
    
    
end