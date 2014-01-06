classdef Jobrunner < handle
    
    properties
        jrID;
        dataPath;
        kernel;
        data;
        startidx;
        thisidxs;
    end
    
    methods
        
        function obj = Jobrunner( jrID, dataPath, NOEXECUTE )
            obj.jrID = jrID;
            obj.dataPath = dataPath;
            cd( obj.dataPath );
            obj.kernel = obj.loadKernel( obj.dataPath );
            [obj.data, obj.startidx, obj.thisidxs]  = obj.load( obj.dataPath, obj.jrID );
            %EXECUTE
            if( nargin == 2 )
                obj.localRunner();
            end
        end
        
        function localRunner( obj )
            
            fprintf( 'Executing Runner ID: %i\n', obj.jrID );
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
            
        end
        
    end
    
    methods (Static)
        
        function [data, startidx, idxs] = load( dataPath, jrID )
            input = load( [dataPath,'input',num2str(jrID),'.mat'] );
            data = input.input;
            startidx = input.startidx;
            if( isfield( input, 'idxs' ) )
                idxs = input.idxs;
            else
                idxs = [];
            end
        end
        
        function save( dataPath, jrID, output )
            save( [dataPath,'output',num2str(jrID),'.mat'], 'output' );
        end
        
        function kernel = loadKernel( dataPath )
            kernel = load( [dataPath,'kernel.mat'] );
            kernel = kernel.kernel;
            if( ~ isa( kernel, 'legion.Kernel' ) )
                error('Kernel Not Specified');
            end
        end
        
    end
    
end