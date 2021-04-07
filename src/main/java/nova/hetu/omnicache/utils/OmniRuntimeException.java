/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nova.hetu.omnicache.utils;

public class OmniRuntimeException extends RuntimeException
{
    private final OmniErrorType errorType;
    public OmniRuntimeException(OmniErrorType errorType,String msg){
        super(msg);
        this.errorType = errorType;
    }

    public OmniRuntimeException(OmniErrorType errorType,String s, Throwable throwable)
    {
        super(s, throwable);
        this.errorType = errorType;
    }

    public OmniRuntimeException(OmniErrorType errorType,Throwable throwable)
    {
        super(throwable);
        this.errorType = errorType;
    }
}
