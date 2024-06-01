<?php

namespace SqlMigration\Core;

class Executor{

    public function __construct(private Connector $connector){}

    public function execute(Executable $executable){
        try{
            $pdo = $this->connector->get();
            
            $pdo->exec($executable->cmd);

            $executable->success();                     
            
        }catch(\Throwable $e){                    
            $executable->error($e->getMessage());                     
        }     
    }

}