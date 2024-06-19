<?php

namespace SqlMigration\Core;

class Executor{
    
    public function __construct(
        private Connector $connector,
        private bool $foreign_key_check
    ){}

    public function execute(Executable $executable){
        $pdo = $this->connector->get();

        try{
            if(!$this->foreign_key_check){
                $pdo->exec('SET FOREIGN_KEY_CHECKS = 0');
            }
            
            $pdo->exec($executable->cmd);

            $executable->success();
            
        }catch(\Throwable $e){                    
            $executable->error($e->getMessage());                     
        }
        finally{
            if(!$this->foreign_key_check){
                $pdo->exec('SET FOREIGN_KEY_CHECKS = 1');
            }
        }
        
    }

}