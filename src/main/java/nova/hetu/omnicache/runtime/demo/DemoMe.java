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
package nova.hetu.omnicache.runtime.demo;

import nova.hetu.omnicache.runtime.UnsafeLongVec;

public class DemoMe
{
    public static void main(String[] args)
    {
        for (int i = 0; i < 1000; i++) {
            UnsafeVec unsafeVecHandler = new UnsafeVec();
            UnsafeLongVec longVec = new UnsafeLongVec(10);
            for (int idx = 0; idx < 10; idx++) {
                longVec.set(idx, idx);
            }
            long address = RustInterface.INSTANCE.toRust(longVec.getAddress(), 10);
//            longVec = new UnsafeLongVec(address, 10);
//            for (int idx = 0; idx < 10; idx++) {
//                System.out.println(longVec.get(idx));
//            }
            try {
                longVec.release();
            }catch (Exception ex){
                ex.printStackTrace();
            }
        }
    }
}
