//
// Created by arpit on 10/8/21.
//

#ifndef OMNI_RUNTIME_CODEGENVALUE_H
#define OMNI_RUNTIME_CODEGENVALUE_H


class CodeGenValue {
public:
    explicit CodeGenValue(llvm::Value* data,
                          llvm::Value *length = nullptr): data(data), length(length) {}

    virtual ~CodeGenValue() = default;
    llvm::Value* Data() {return data;}
    llvm::Value* Length() {return length;}

    void SetData(llvm::Value* data) {
        this->data = data;
    }


private:
    llvm::Value* data;
    llvm::Value* length;
};


#endif //OMNI_RUNTIME_CODEGENVALUE_H

